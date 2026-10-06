require 'thread'

module MQTT
  module SN
    # A MQTT-SN client session that runs on top of a datagram transport.
    #
    # The transport is provided by the caller and must respond to:
    #   send(String)      :: write one datagram to the fixed gateway
    #   recv(Numeric|nil) :: receive one datagram, waiting at most the
    #                        given number of seconds; nil means wait forever
    #   close()           :: release any underlying resources
    #
    # The clock is also injected, and must respond to #now and return a
    # monotonically increasing number of seconds.
    class Client
      # Fixed gateway transport used to send and receive datagrams
      attr_reader :transport

      # Client identifier sent to the gateway
      attr_accessor :client_id

      # Time (in seconds) between keep-alive PINGREQ packets (default 15)
      attr_accessor :keep_alive

      # Number of seconds to wait for acknowledgement packets (default 5)
      attr_accessor :ack_timeout

      # Set the 'Clean Session' flag when connecting? (default true)
      attr_accessor :clean_session

      # Clock used for keep-alive and sleep duration calculations
      attr_reader :clock

      # Session state, one of :disconnected, :connecting, :active,
      # :asleep or :awake (the latter means a wake-up drain is in progress)
      attr_reader :state

      # Messages that arrived while the client was sleeping and which have
      # not yet been delivered by #get_packet
      attr_reader :queued_messages

      # Minimum time to block in the transport when polling (seconds)
      SELECT_TIMEOUT = 0.1

      # Create and connect a new MQTT-SN Client.
      def self.connect(transport, attributes = {}, &block)
        client = new(transport, attributes)
        client.connect(&block)
        client
      end

      # Create a new MQTT-SN Client session.
      #
      # The first argument is the fixed-gateway datagram transport.
      # The optional attributes hash may contain :client_id, :keep_alive,
      # :ack_timeout, :clean_session and :clock.
      def initialize(transport, attributes = {})
        @transport = transport
        @client_id = attributes[:client_id]
        @keep_alive = attributes.fetch(:keep_alive, 15)
        @ack_timeout = attributes.fetch(:ack_timeout, 5)
        @clean_session = attributes.fetch(:clean_session, true)
        @clock = attributes[:clock] || MonotonicClock.new

        @state = :disconnected
        @last_ping_request = nil
        @last_ping_response = nil
        @sleep_deadline = nil
        @last_packet_id = 0
        @inbound = []
        @queued_messages = []
        @write_semaphore = Mutex.new
      end

      # True while a session exists, including while asleep or waking up.
      def connected?
        @state != :disconnected
      end

      # True while the client is allowed to exchange application packets.
      def active?
        @state == :active
      end

      # True while the client is sleeping at the gateway.
      def asleep?
        @state == :asleep
      end

      # Seconds of the granted sleep duration that remain, according to the
      # injected clock. Returns nil when the client is not asleep.
      def sleep_remaining
        return nil unless asleep?

        @sleep_deadline - @clock.now
      end

      # Connect to the fixed MQTT-SN gateway.
      # If a block is given, it is executed and the session is closed afterwards.
      def connect(clientid = nil)
        @client_id = clientid unless clientid.nil?
        if @client_id.nil? || @client_id.empty?
          raise ArgumentError, 'Must provide a client_id for a MQTT-SN client'
        end
        raise MQTT::SN::NotConnectedException, 'Already connected' if connected?

        @state = :connecting
        packet = Packet::Connect.new(
          :clean_session => @clean_session,
          :keep_alive => @keep_alive,
          :client_id => @client_id
        )
        send_packet(packet)

        connack =
          begin
            wait_for_event(:connack)
          rescue MQTT::SN::TimeoutException
            fail_session
            @transport.close
            raise
          end
        unless connack.return_code.zero?
          fail_session
          @transport.close
          raise MQTT::SN::ProtocolException, connack.return_msg
        end

        now = @clock.now
        @last_ping_request = now
        @last_ping_response = now
        @sleep_deadline = nil
        @state = :active

        return unless block_given?

        begin
          yield(self)
        ensure
          disconnect
        end
      end

      # Publish a message. +topic+ may be a two-character short topic name
      # (used as a short topic id) or a previously registered numeric topic id.
      #
      # QoS 0 and QoS -1 (fire-and-forget) complete immediately.
      # QoS 1 waits for the matching PUBACK and raises on timeout.
      def publish(topic, payload = '', retain = false, qos = 0)
        raise MQTT::SN::NotConnectedException unless active?
        unless qos.nil? || (-1..1).cover?(qos)
          raise ArgumentError, "Unsupported QoS level: #{qos}"
        end

        packet = Packet::Publish.new(
          :topic_id => topic,
          :topic_id_type => topic_type(topic),
          :data => payload.to_s,
          :retain => retain,
          :qos => qos,
          :id => qos.to_i == 1 ? next_packet_id : 0
        )
        send_packet(packet)
        return nil unless qos.to_i == 1

        puback =
          begin
            wait_for_event(:puback, packet.id)
          rescue MQTT::SN::TimeoutException
            fail_session
            @transport.close
            raise
          end
        unless puback.return_code.zero?
          raise MQTT::SN::ProtocolException,
                "Gateway rejected PUBLISH with return code #{puback.return_code}"
        end
        puback
      end

      # Return the next application message received from the gateway.
      #
      # Returns a [topic_id, data] pair, or nil if the optional timeout
      # expires before a message arrives. With no timeout the call blocks
      # and performs keep-alive while waiting.
      def get(timeout = nil)
        packet = get_packet(timeout)
        return nil if packet.nil?

        [packet.topic_id, packet.data]
      end

      # Like #get but returns the raw MQTT::SN::Packet::Publish object.
      def get_packet(timeout = nil)
        deadline = timeout.nil? ? nil : @clock.now + timeout
        packet = nil
        loop do
          packet = @queued_messages.shift || @inbound.shift
          break unless packet.nil?

          event = read_one(deadline)
          return nil if event.nil? && deadline && @clock.now >= deadline
        end
        packet
      end

      # Enter the MQTT-SN sleep state.
      #
      # A DISCONNECT carrying the requested sleep duration (seconds) is sent
      # and the gateway answers with a DISCONNECT containing the granted
      # duration. No normal keep-alive PINGREQ packets are sent while asleep.
      def sleep(duration)
        raise MQTT::SN::NotConnectedException, 'Must be connected to sleep' unless active?
        unless duration.is_a?(Integer) && duration > 0
          raise ArgumentError, 'Sleep duration must be a positive Integer (seconds)'
        end

        send_packet(Packet::Disconnect.new(:duration => duration))
        ack = wait_for_event(:sleep_ack)

        @sleep_deadline = @clock.now + ack.duration
        @state = :asleep
        ack.duration
      end

      # Wake a sleeping client up.
      #
      # Sends a PINGREQ carrying the client identifier and then drains the
      # messages that the gateway buffered while the client was sleeping,
      # stopping at the matching PINGRESP. Messages may be consumed with a
      # block or are returned as an array.
      def wake
        raise MQTT::SN::NotConnectedException, 'Can only wake up from sleep' unless asleep?

        @state = :awake
        drained = []
        @queued_messages = []
        send_packet(Packet::Pingreq.new(:client_id => @client_id))
        begin
          loop do
            event = wait_for_event(:wake)
            break if event == :pingresp

            drained << event
            @queued_messages << event
            yield(event) if block_given?
          end
        rescue MQTT::SN::TimeoutException
          # No reply at all: the client is still sleeping as far as it knows.
          @state = :asleep
          raise
        end

        now = @clock.now
        @last_ping_request = now
        @last_ping_response = now
        @sleep_deadline = nil
        @state = :active
        drained
      end

      # End the client session.
      #
      # An active client sends a duration-less DISCONNECT. A sleeping client
      # cannot be reached over the datagram transport, so no packet is sent.
      def disconnect
        return unless connected?

        if active?
          send_packet(Packet::Disconnect.new)
        end
        @state = :disconnected
        @sleep_deadline = nil
        @transport.close
      end

      private

      # Wait for and process a single datagram. Returns the classified event
      # or nil if the transport timeout expired. Performs keep-alive while
      # in the active state.
      def read_one(deadline = nil)
        if @state == :active
          ping = keep_alive_tick
          return ping unless ping.nil?
        end

        wait_time = idle_wait_time(deadline)
        buffer = @transport.recv(wait_time)
        return nil if buffer.nil?

        classify(Packet.parse(buffer))
      end

      # Send an on-line keep-alive PINGREQ if one is due. Returns the symbol
      # :sent_ping when one was sent, nil otherwise.
      def keep_alive_tick
        return nil unless @keep_alive.is_a?(Integer) && @keep_alive > 0

        if @clock.now - @last_ping_response > (@keep_alive * 1.5)
          @state = :disconnected
          raise MQTT::SN::ProtocolException,
                "No PINGRESP received within #{(@keep_alive * 1.5).ceil} seconds"
        end
        return nil unless @clock.now - @last_ping_request >= @keep_alive

        send_packet(Packet::Pingreq.new)
        @last_ping_request = @clock.now
        :sent_ping
      end

      # How long the next transport read may block: never longer than needed
      # to fire the next keep-alive PINGREQ, or the caller's deadline.
      def idle_wait_time(deadline)
        next_ping = @last_ping_request + @keep_alive if @state == :active && @keep_alive.to_i > 0
        limits = []
        limits << deadline unless deadline.nil?
        limits << next_ping if next_ping
        return nil if limits.empty?

        remaining = limits.min - @clock.now
        remaining > SELECT_TIMEOUT ? remaining : SELECT_TIMEOUT
      end

      # Pump datagrams until the requested event is classified.
      def wait_for_event(name, id = nil, timeout = @ack_timeout)
        deadline = timeout.nil? ? nil : @clock.now + timeout
        loop do
          event = read_one(deadline)
          if event.nil?
            next if deadline.nil?
            raise MQTT::SN::TimeoutException,
                  "Timed out after #{timeout} seconds waiting for #{name}"
          end
          next unless matching_event?(name, id, event)

          return extract_event(name, event)
        end
      end

      def matching_event?(name, id, event)
        case name
        when :connack
          event.is_a?(Packet::Connack)
        when :puback
          event.is_a?(Packet::Puback) && event.id == id
        when :sleep_ack
          event.is_a?(Packet::Disconnect)
        when :wake
          event.is_a?(Packet::Publish) || event == :pingresp
        end
      end

      def extract_event(name, event)
        name == :wake && event == :pingresp ? :pingresp : event
      end

      # Classify a decoded packet according to the current session state.
      # Returns the packet or a symbol for stateful events.
      def classify(packet)
        case packet
        when Packet::Pingresp
          @last_ping_response = @clock.now
          :pingresp
        when Packet::Disconnect
          gateway_disconnect(packet)
        when Packet::Publish
          receive_publish(packet)
        when Packet::Puback
          packet
        when Packet::Connack
          packet
        else
          # Late, duplicate or otherwise unexpected acknowledgement:
          # ignore it rather than advancing session state.
          nil
        end
      end

      def receive_publish(packet)
        if @state == :active || @state == :awake
          if packet.qos.to_i == 1
            send_packet(Packet::Puback.new(
                          :topic_id => packet.topic_id,
                          :topic_id_type => packet.topic_id_type,
                          :id => packet.id,
                          :return_code => 0
                        ))
          end
          @inbound << packet if @state == :active
          packet
        else
          # No session at the gateway for us: drop the datagram.
          nil
        end
      end

      def gateway_disconnect(packet)
        if packet.duration && (active? || @state == :connecting)
          # The gateway grants the requested sleep duration.
          packet
        else
          @state = :disconnected
          @sleep_deadline = nil
          @transport.close
          raise MQTT::SN::SessionLostException,
                'Gateway closed the MQTT-SN session'
        end
      end

      def send_packet(packet)
        @write_semaphore.synchronize do
          @transport.send(packet.to_s)
        end
      end

      def fail_session
        @state = :disconnected
        @sleep_deadline = nil
      end

      def topic_type(topic)
        topic.is_a?(String) ? :short : :normal
      end

      def next_packet_id
        @last_packet_id += 1
        @last_packet_id = 1 if @last_packet_id > 0xffff
        @last_packet_id
      end

      # Default clock backed by the monotonic process clock.
      class MonotonicClock
        def now
          Process.clock_gettime(Process::CLOCK_MONOTONIC)
        end
      end
    end
  end
end

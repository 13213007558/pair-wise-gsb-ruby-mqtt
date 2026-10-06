# encoding: BINARY

module MQTT
  module SN
    # Raised when the gateway does not answer a request in time,
    # even after retransmissions
    class TimeoutException < MQTT::Exception
    end

    # Raised when the gateway rejects a connection attempt
    class ConnectException < MQTT::Exception
    end

    # Raised when the gateway indicates it no longer has our session
    class SessionLostException < MQTT::Exception
    end

    # A client session with a single, fixed MQTT-SN gateway.
    #
    # The session drives the MQTT::SN::Packet classes over a datagram
    # transport (e.g. UDP) supplied by the caller, and gets all of its
    # timing from a caller-supplied clock, so it works just as well in
    # tests as on a real network.
    #
    # The transport must provide:
    #   send(bytes)  -- transmit one datagram to the gateway
    #   recv         -- return the next received datagram as a String,
    #                   or nil if none is currently available
    #
    # The clock must provide:
    #   now             -- monotonic time in seconds (Float)
    #   sleep(seconds)  -- wait without busy-spinning
    #
    # Supported flows: CONNECT/CONNACK, QoS 0 and 1 publish in both
    # directions, SUBSCRIBE, sleeping (DISCONNECT with a duration),
    # waking up (PINGREQ with the ClientId, draining buffered messages
    # until PINGRESP) and keep-alive pings while awake.
    #
    # State changes and failures are observable through the +state+
    # reader and the +on_event+ callback:
    #   :connected, :disconnected, :asleep, :awake,
    #   :timeout, :session_lost, :message
    class Session
      # One of :offline, :awake, :asleep, :lost or :closed
      attr_reader :state

      attr_reader :client_id
      attr_reader :keep_alive

      # Seconds the device asked to sleep for
      attr_reader :sleep_duration

      # Called with (event, *details) whenever something noteworthy happens
      attr_accessor :on_event

      # Called with a MQTT::SN::Packet::Publish for every incoming message
      attr_accessor :on_message

      DEFAULTS = {
        :keep_alive => 30,   # seconds between heartbeats while awake
        :ack_timeout => 5,   # seconds to wait for any gateway reply
        :max_retries => 3,   # retransmissions before giving up
        :poll_interval => 0.01
      }

      def initialize(args = {})
        config = DEFAULTS.merge(args)
        @transport = config.fetch(:transport)
        @clock = config.fetch(:clock)
        @client_id = config.fetch(:client_id)
        @keep_alive = config[:keep_alive]
        @ack_timeout = config[:ack_timeout]
        @max_retries = config[:max_retries]
        @poll_interval = config[:poll_interval]
        @state = :offline
        @sleep_duration = nil
        @last_activity = @clock.now
        @next_message_id = 1
        @acked_publishes = []
      end

      # Connect to the gateway; returns true on success.
      # Raises MQTT::SN::ConnectException if the gateway rejects us and
      # MQTT::SN::TimeoutException if the gateway never answers.
      def connect(clean_session: true)
        raise 'Session is already open' unless @state == :offline

        packet = MQTT::SN::Packet::Connect.new(
          :client_id => @client_id,
          :keep_alive => @keep_alive,
          :clean_session => clean_session
        )
        connack = request(packet, MQTT::SN::Packet::Connack)
        unless connack.return_code == 0x00
          raise MQTT::SN::ConnectException, connack.return_msg
        end

        change_state(:awake)
        emit(:connected)
        true
      end

      # Subscribe to a topic name; returns the topic id granted by
      # the gateway, for use with #publish.
      def subscribe(topic_name, qos: 0)
        ensure_awake!
        id = next_message_id
        packet = MQTT::SN::Packet::Subscribe.new(
          :id => id,
          :topic_name => topic_name,
          :topic_id_type => :normal,
          :qos => qos
        )
        suback = request(packet, MQTT::SN::Packet::Suback) { |p| p.id == id }
        unless suback.return_code == 0x00
          raise MQTT::SN::ProtocolException, "Subscribe rejected: #{suback.return_code}"
        end

        suback.topic_id
      end

      # Publish a message at QoS 0 (fire and forget) or QoS 1
      # (retransmitted until the gateway's PUBACK arrives).
      def publish(topic_id, data, qos: 0, retain: false, topic_id_type: :normal)
        ensure_awake!
        id = qos == 1 ? next_message_id : 0
        packet = MQTT::SN::Packet::Publish.new(
          :topic_id => topic_id,
          :topic_id_type => topic_id_type,
          :id => id,
          :data => data,
          :qos => qos,
          :retain => retain
        )
        if qos == 1
          request(packet, MQTT::SN::Packet::Puback) { |p| p.id == id }
        else
          transmit(packet)
        end
        id
      end

      # Process every datagram that has already arrived, without
      # sending anything. Returns the number of packets handled.
      def poll
        handled = 0
        while (datagram = @transport.recv)
          packet = MQTT::SN::Packet.parse(datagram)
          @last_activity = @clock.now
          dispatch(packet)
          handled += 1
        end
        handled
      end

      # Should be called regularly by the caller. Processes incoming
      # datagrams and sends a keep-alive PINGREQ when the keep-alive
      # interval has elapsed without any traffic. No keep-alives are
      # sent unless the session is awake -- in particular, none are
      # sent while the device sleeps.
      def tick
        poll if @state == :awake || @state == :asleep
        return self unless @state == :awake
        return self unless @clock.now - @last_activity >= @keep_alive

        begin
          request(MQTT::SN::Packet::Pingreq.new, MQTT::SN::Packet::Pingresp)
        rescue MQTT::SN::TimeoutException
          # A gateway that stops answering heartbeats is gone
          lose_session(:keepalive_timeout)
        end
        self
      end

      # Go to sleep for +duration+ seconds. Sends DISCONNECT with the
      # duration and waits for the gateway to acknowledge it.
      def sleep(duration)
        ensure_awake!
        packet = MQTT::SN::Packet::Disconnect.new(:duration => duration)
        request(packet, MQTT::SN::Packet::Disconnect)
        @sleep_duration = duration
        change_state(:asleep)
        emit(:asleep, duration)
        self
      end

      # Wake up from sleep: send PINGREQ with our ClientId and deliver
      # the messages the gateway buffered for us, until its PINGRESP
      # marks the end of the backlog.
      def wake
        raise 'Session is not asleep' unless @state == :asleep

        packet = MQTT::SN::Packet::Pingreq.new(:client_id => @client_id)
        request(packet, MQTT::SN::Packet::Pingresp)
        @sleep_duration = nil
        change_state(:awake)
        emit(:awake)
        self
      rescue MQTT::SN::TimeoutException
        # The gateway no longer knows us (or is gone)
        lose_session(:wake_timeout)
        self
      end

      # Client-initiated shutdown. Sends DISCONNECT without a duration
      # and closes the session, regardless of whether the gateway
      # acknowledges it in time.
      def disconnect
        return self if @state == :offline || @state == :closed

        was_asleep = @state == :asleep
        begin
          request(MQTT::SN::Packet::Disconnect.new, MQTT::SN::Packet::Disconnect)
        rescue MQTT::SN::TimeoutException, MQTT::SN::SessionLostException
          # The gateway is gone; the client is ending the session anyway
        end
        @sleep_duration = nil
        change_state(:closed)
        emit(:disconnected, was_asleep)
        self
      end

      # True while the session can exchange application messages
      def awake?
        @state == :awake
      end

      # True while the device is sleeping at the gateway
      def asleep?
        @state == :asleep
      end

      private

      def change_state(new_state)
        @state = new_state
      end

      def emit(event, *details)
        @on_event.call(event, *details) if @on_event
      end

      def lose_session(reason)
        change_state(:lost)
        emit(:session_lost, reason)
      end

      def ensure_awake!
        case @state
        when :awake
          nil
        when :asleep
          raise 'Session is asleep; call wake first'
        else
          raise MQTT::NotConnectedException, "Session is #{@state}"
        end
      end

      def next_message_id
        id = @next_message_id
        @next_message_id = @next_message_id >= 0xFFFF ? 1 : @next_message_id + 1
        id
      end

      def transmit(packet)
        @transport.send(packet.to_s)
        @last_activity = @clock.now
      end

      # Send a request packet and wait for a matching reply,
      # retransmitting on timeout. Unrelated packets received while
      # waiting are dispatched (incoming PUBLISH, gateway DISCONNECT);
      # duplicate or late replies that match nothing are ignored.
      def request(packet, reply_class, &predicate)
        attempt = 0
        matched = nil
        begin
          attempt += 1
          transmit(packet)
          matched = wait_for(reply_class, &predicate)
        rescue MQTT::SN::TimeoutException
          emit(:timeout, packet.class.name.split('::').last, attempt)
          retry if attempt <= @max_retries && !session_gone?
          raise
        end
        matched
      end

      def session_gone?
        @state == :lost || @state == :closed
      end

      # Wait up to @ack_timeout seconds for a packet of +reply_class+
      # (optionally filtered by a block), handling anything else that
      # arrives in the meantime.
      def wait_for(reply_class, &filter)
        deadline = @clock.now + @ack_timeout
        loop do
          datagram = @transport.recv
          if datagram
            packet = MQTT::SN::Packet.parse(datagram)
            @last_activity = @clock.now
            if packet.is_a?(reply_class) && (filter.nil? || filter.call(packet))
              return packet
            end
            dispatch(packet)
          end
          raise MQTT::SN::TimeoutException, "Timed out waiting for #{reply_class}" if @clock.now >= deadline
          raise MQTT::SN::SessionLostException, 'Gateway terminated the session' if @state == :lost

          @clock.sleep(@poll_interval)
        end
      end

      # Handle a packet that did not match the pending request.
      # Duplicate or late replies are dropped here and must never
      # move the session state machine forward.
      def dispatch(packet)
        case packet
        when MQTT::SN::Packet::Publish
          deliver(packet)
        when MQTT::SN::Packet::Disconnect
          # An unsolicited DISCONNECT means the gateway has dropped us
          lose_session(:gateway_disconnect) if @state == :awake || @state == :asleep
        when MQTT::SN::Packet::Pingresp,
             MQTT::SN::Packet::Connack,
             MQTT::SN::Packet::Suback,
             MQTT::SN::Packet::Puback,
             MQTT::SN::Packet::Regack
          # Late or duplicate reply to a request that already
          # completed; safe to ignore
          nil
        end
      end

      def deliver(packet)
        if packet.qos == 1
          # Always (re-)acknowledge, but only deliver each message id once
          transmit(MQTT::SN::Packet::Puback.new(:topic_id => packet.topic_id, :id => packet.id))
          return if @acked_publishes.include?(packet.id)

          @acked_publishes << packet.id
          @acked_publishes.shift while @acked_publishes.length > 32
        end
        emit(:message, packet)
        @on_message.call(packet) if @on_message
      end
    end
  end
end

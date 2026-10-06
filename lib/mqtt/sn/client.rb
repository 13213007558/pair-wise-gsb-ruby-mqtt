module MQTT
  module SN
    # Lightweight MQTT-SN client state machine.
    #
    # Currently implemented:
    #   * Gateway discovery (SEARCHGW/GWINFO/ADVERTISE)
    #   * Connecting to a gateway (CONNECT/CONNACK)
    #   * Topic name registration (REGISTER/REGACK)
    #   * Publishing with QoS 0 and QoS 1 (PUBLISH/PUBACK)
    #
    # Sleeping clients and QoS -1 publishing are not supported:
    # packets related to them are answered with the :not_supported
    # status.
    #
    # The client performs no I/O itself. Outgoing packets are handed
    # to a caller supplied UDP transport object that must implement:
    #
    #   transport.send(data, address)
    #
    # where +data+ is the serialised packet and +address+ is the
    # gateway address (or nil for a broadcast during discovery).
    # Incoming datagrams should be fed back into the client with
    # #receive or #receive_packet.
    class Client
      # A gateway discovered via GWINFO or ADVERTISE
      class Gateway
        attr_accessor :id       # Gateway identifier (Integer)
        attr_accessor :address  # Transport specific address of the gateway
        attr_accessor :duration # Keep-alive duration advertised by the gateway

        def initialize(id, address = nil, duration = nil)
          @id = id
          @address = address
          @duration = duration
        end

        def ==(other)
          other.is_a?(Gateway) && other.id == id && other.address == address
        end
      end

      # Default values for client options
      DEFAULTS = {
        :client_id => nil,
        :keep_alive => 15,
        :clean_session => true,
        :transport => nil,
        :logger => nil
      }

      attr_reader :state         # One of :disconnected, :searching, :connecting or :connected
      attr_reader :gateways      # Array of discovered MQTT::SN::Client::Gateway objects
      attr_reader :gateway       # The gateway the client is currently connected to
      attr_reader :client_id     # The client identifier sent in CONNECT
      attr_accessor :keep_alive  # Keep-alive time in seconds
      attr_accessor :clean_session # When true, the gateway deletes subscriptions on disconnect
      attr_accessor :transport   # Caller supplied UDP transport
      attr_writer :logger

      def initialize(args = {})
        args = DEFAULTS.merge(args)
        @client_id = args[:client_id] || generate_client_id
        @keep_alive = args[:keep_alive]
        @clean_session = args[:clean_session]
        @transport = args[:transport]
        @logger = args[:logger]
        @state = :disconnected
        @gateways = []
        @gateway = nil
        @pending_gateway = nil
        @last_message_id = 0
        @registrations = {}
        @pending = {}
        @publish_queue = Hash.new { |hash, key| hash[key] = [] }
      end

      def logger
        @logger ||= Logger.new(STDOUT)
      end

      # Broadcast a SEARCHGW packet to discover gateways.
      # Replies should be passed to #receive_packet.
      def search_gateway(radius = 1)
        packet = MQTT::SN::Packet::Searchgw.new(:radius => radius)
        @state = :searching if @state == :disconnected
        send_packet(packet, nil)
        packet
      end

      # Connect to a previously discovered gateway, identified by its id
      def connect(gateway_id)
        gateway = @gateways.find { |gw| gw.id == gateway_id }
        raise ArgumentError, "Unknown gateway id: #{gateway_id.inspect}" if gateway.nil?

        @pending_gateway = gateway
        packet = MQTT::SN::Packet::Connect.new(
          :client_id => client_id,
          :keep_alive => keep_alive,
          :clean_session => clean_session
        )
        @state = :connecting
        send_packet(packet, gateway.address)
        packet
      end

      # Disconnect from the current gateway
      def disconnect
        check_connected
        packet = MQTT::SN::Packet::Disconnect.new
        address = @gateway.address
        @state = :disconnected
        @gateway = nil
        send_packet(packet, address)
        packet
      end

      # Register a normal topic name with the current gateway.
      # Returns the topic id if already registered, otherwise the
      # message id of the REGISTER packet that was sent.
      def register_topic(topic_name)
        check_connected
        topic_id = registered_topic_id(topic_name)
        return topic_id unless topic_id.nil?

        id = next_message_id
        @pending[id] = [:register, topic_name]
        packet = MQTT::SN::Packet::Register.new(
          :id => id,
          :topic_id => 0,
          :topic_name => topic_name
        )
        send_packet(packet, @gateway.address)
        id
      end

      # Look up the topic id assigned by the current gateway for a
      # normal topic name. Returns nil if it isn't registered.
      # Registrations are per gateway: after connecting to a different
      # gateway, ids from the previous gateway are never reused.
      def registered_topic_id(topic_name)
        @registrations[topic_name]
      end

      # Publish a message to a topic.
      #
      # The +topic+ argument depends on the :topic_id_type option:
      #   :normal     => a topic name String (registered on demand)
      #   :predefined => a topic id Integer known to the gateway
      #   :short      => a two character topic name String
      #
      # Only QoS 0 and QoS 1 are supported. For QoS 1 the message id
      # of the PUBLISH packet is returned and the Puback should be
      # passed to #receive_packet. For a normal topic name that is not
      # registered yet, :registering is returned and the publish is
      # sent once the REGACK arrives.
      def publish(topic, data, options = {})
        check_connected
        options = {
          :qos => 0,
          :retain => false,
          :topic_id_type => :normal
        }.merge(options)

        unless [0, 1].include?(options[:qos])
          raise ArgumentError, "QoS #{options[:qos]} is not supported, only QoS 0 and 1"
        end

        case options[:topic_id_type]
        when :normal
          topic_id = registered_topic_id(topic)
          if topic_id.nil?
            @publish_queue[topic] << [data, options]
            register_topic(topic)
            return :registering
          end
          send_publish(topic_id, data, options)
        when :predefined
          unless topic.is_a?(Integer)
            raise ArgumentError, 'topic must be an Integer topic id for :predefined topics'
          end
          send_publish(topic, data, options)
        when :short
          unless topic.is_a?(String) && topic.length == 2
            raise ArgumentError, 'topic must be a two character String for :short topics'
          end
          send_publish(topic, data, options)
        else
          raise ArgumentError, "Invalid topic_id_type: #{options[:topic_id_type].inspect}"
        end
      end

      # Parse a raw datagram and feed it into the state machine
      def receive(data, from_address = nil)
        receive_packet(MQTT::SN::Packet.parse(data), from_address)
      end

      # Feed a received packet into the state machine.
      # Returns a status symbol:
      #   :gateway_found, :connected, :registered, :published,
      #   :disconnected, :rejected, :not_supported, :ok or :ignored
      def receive_packet(packet, from_address = nil)
        logger.debug("Received #{packet.class} from #{from_address.inspect}")
        case packet
        when MQTT::SN::Packet::Advertise
          add_gateway(packet.gateway_id, from_address, packet.duration)
          :gateway_found
        when MQTT::SN::Packet::Gwinfo
          add_gateway(packet.gateway_id, packet.gateway_address || from_address)
          :gateway_found
        when MQTT::SN::Packet::Connack
          handle_connack(packet)
        when MQTT::SN::Packet::Regack
          handle_regack(packet)
        when MQTT::SN::Packet::Puback
          handle_puback(packet)
        when MQTT::SN::Packet::Publish
          handle_incoming_publish(packet)
        when MQTT::SN::Packet::Disconnect
          handle_disconnect(packet)
        when MQTT::SN::Packet::Willtopicreq, MQTT::SN::Packet::Willmsgreq
          :not_supported
        when MQTT::SN::Packet::Pingresp
          :ok
        else
          :ignored
        end
      end

      private

      def generate_client_id
        "ruby-mqtt-sn-#{rand(1_000_000_000)}"
      end

      def check_connected
        raise MQTT::NotConnectedException, 'Not connected to a gateway' unless @state == :connected
      end

      def next_message_id
        @last_message_id += 1
        @last_message_id = 1 if @last_message_id > 0xFFFF
        @last_message_id
      end

      def send_packet(packet, address)
        raise 'No transport configured' if transport.nil?

        logger.debug("Sending #{packet.class} to #{address.inspect}")
        transport.send(packet.to_s, address)
      end

      def add_gateway(id, address, duration = nil)
        gateway = @gateways.find { |gw| gw.id == id }
        if gateway.nil?
          gateway = Gateway.new(id, address, duration)
          @gateways << gateway
        else
          gateway.address = address unless address.nil?
          gateway.duration = duration unless duration.nil?
        end
        gateway
      end

      def handle_connack(packet)
        if packet.return_code == 0x00
          previous_gateway = @gateway
          @gateway = @pending_gateway
          @pending_gateway = nil
          if previous_gateway && @gateway && previous_gateway.id != @gateway.id
            # Topic id registrations are only valid for the gateway
            # that issued them, so never reuse them after switching
            clear_registrations
          end
          @state = :connected
          :connected
        else
          @pending_gateway = nil
          @state = :disconnected
          :rejected
        end
      end

      def handle_regack(packet)
        request = @pending.delete(packet.id)
        return :ignored if request.nil? || request[0] != :register

        topic_name = request[1]
        if packet.return_code == 0x00
          @registrations[topic_name] = packet.topic_id
          flush_publish_queue(topic_name)
          :registered
        else
          @publish_queue.delete(topic_name)
          :rejected
        end
      end

      def handle_puback(packet)
        request = @pending.delete(packet.id)
        return :ignored if request.nil? || request[0] != :publish

        packet.return_code == 0x00 ? :published : :rejected
      end

      def handle_incoming_publish(packet)
        if packet.qos == -1
          # QoS -1 publishing is not supported by this client
          :not_supported
        else
          :ignored
        end
      end

      def handle_disconnect(packet)
        if packet.duration
          # Sleeping clients are not supported by this client
          :not_supported
        else
          @state = :disconnected
          @gateway = nil
          :disconnected
        end
      end

      def clear_registrations
        @registrations.clear
        @pending.delete_if { |_id, request| request[0] == :register }
        @publish_queue.clear
      end

      def flush_publish_queue(topic_name)
        queue = @publish_queue.delete(topic_name)
        return if queue.nil?

        queue.each do |data, options|
          send_publish(@registrations[topic_name], data, options)
        end
      end

      def send_publish(topic_id, data, options)
        id = options[:qos] == 1 ? next_message_id : 0
        packet = MQTT::SN::Packet::Publish.new(
          :id => id,
          :qos => options[:qos],
          :retain => options[:retain],
          :topic_id_type => options[:topic_id_type],
          :topic_id => topic_id,
          :data => data
        )
        @pending[id] = [:publish, topic_id] if options[:qos] == 1
        send_packet(packet, @gateway.address)
        id
      end
    end
  end
end

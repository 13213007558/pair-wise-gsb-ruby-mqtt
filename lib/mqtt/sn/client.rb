# encoding: BINARY

module MQTT
  module SN
    # A lightweight MQTT-SN client state machine.
    #
    # The client performs no network I/O of its own. Outbound datagrams
    # are serialised and handed to a caller-provided UDP transport (any
    # object responding to +send_data+) and inbound packets are fed to
    # the client using #handle_packet.
    #
    # Supported: gateway discovery, connecting, REGISTER/REGACK and
    # publishing at QoS 0 and 1 using normal, predefined or short topic
    # identifiers. Sleeping clients and QoS -1 are not supported: when
    # packets related to either are received, the client status is set
    # to :unsupported with an explanatory message.
    class Client
      DEFAULT_KEEP_ALIVE = 15

      # All of the possible client states
      STATES = [:offline, :discovering, :disconnected, :connecting, :connected].freeze

      attr_reader :client_id       # Client identifier sent in the CONNECT packet
      attr_reader :keep_alive      # Keep alive period in seconds
      attr_reader :state           # One of the symbols in STATES
      attr_reader :gateway_id      # Identifier of the currently selected gateway
      attr_reader :gateways        # Hash of known gateways (gateway id => duration)
      attr_reader :status          # :ok or :unsupported
      attr_reader :status_message  # Explanation when status is :unsupported
      attr_reader :last_error      # Last error message reported by the gateway
      attr_reader :pending_pubacks # Hash of QoS 1 publishes waiting for a PUBACK

      # Create a new MQTT-SN client state machine.
      #
      # +transport+ is a caller-provided UDP transport object that must
      # respond to +send_data+ with a binary datagram string.
      def initialize(transport, args = {})
        unless transport.respond_to?(:send_data)
          raise ArgumentError, 'transport must respond to send_data'
        end

        @transport = transport
        @client_id = args[:client_id] || generate_client_id
        @keep_alive = args[:keep_alive] || DEFAULT_KEEP_ALIVE
        @clean_session = args.fetch(:clean_session, true)
        @state = :offline
        @gateway_id = nil
        @gateways = {}
        @registrations = {}
        @pending_registrations = {}
        @pending_pubacks = {}
        @queued_publishes = []
        @next_message_id = 1
        @status = :ok
        @status_message = nil
        @last_error = nil
      end

      # Broadcast a SEARCHGW packet and wait for gateways to announce
      # themselves with GWINFO or ADVERTISE packets.
      def discover(radius = 1)
        reset_status
        @state = :discovering
        transmit(MQTT::SN::Packet::Searchgw.new(:radius => radius))
      end

      # Select the gateway to communicate with. Topic ID registrations
      # are issued per-gateway, so switching gateway discards all
      # existing registrations and in-flight messages.
      def use_gateway(gateway_id)
        return if @gateway_id == gateway_id

        @gateway_id = gateway_id
        reset_gateway_state
        @state = :disconnected unless @state == :offline
      end

      # Send a CONNECT packet to the currently selected gateway
      def connect
        if @gateway_id.nil?
          raise MQTT::Exception, 'No gateway selected: call discover or use_gateway first'
        end

        reset_status
        @state = :connecting
        transmit(
          MQTT::SN::Packet::Connect.new(
            :client_id => @client_id,
            :keep_alive => @keep_alive,
            :clean_session => @clean_session
          )
        )
      end

      # Send a DISCONNECT packet to the gateway
      def disconnect
        transmit(MQTT::SN::Packet::Disconnect.new)
        reset_gateway_state if @clean_session
        @state = :offline
      end

      # Returns true if the client is currently connected to a gateway
      def connected?
        @state == :connected
      end

      # Register a normal topic name with the gateway.
      # Returns the REGISTER message id, or nil if the topic is already
      # registered or a registration is already in flight.
      def register(topic_name)
        ensure_connected
        reset_status
        return nil if @registrations.key?(topic_name)
        return nil if @pending_registrations.value?(topic_name)

        message_id = next_message_id
        @pending_registrations[message_id] = topic_name
        transmit(
          MQTT::SN::Packet::Register.new(
            :id => message_id,
            :topic_id => 0x0000,
            :topic_name => topic_name
          )
        )
        message_id
      end

      # Returns true if a normal topic name has been registered with
      # the current gateway
      def registered?(topic_name)
        @registrations.key?(topic_name)
      end

      # Get the gateway-assigned topic ID for a normal topic name
      def topic_id_for(topic_name)
        @registrations[topic_name]
      end

      # Publish a message to the gateway at QoS 0 or 1.
      #
      # The +topic_type+ argument selects the topic representation:
      # * +:normal+ - a topic name string, registered with the gateway
      #   on first use (the publish is queued until the REGACK arrives)
      # * +:predefined+ - an Integer topic ID known by both peers
      # * +:short+ - a two character topic name, used as-is
      #
      # Returns the publish message id (QoS 1) or 0 (QoS 0).
      def publish(topic, data, args = {})
        ensure_connected
        reset_status
        qos = args[:qos] || 0
        retain = args[:retain] || false
        topic_type = args[:topic_type] || :normal

        if qos == -1
          return mark_unsupported('QoS -1 publishing is not supported')
        end
        unless [0, 1].include?(qos)
          raise ArgumentError, "Unsupported QoS level: #{qos}"
        end

        case topic_type
        when :normal
          publish_normal(topic, data, qos, retain)
        when :predefined
          unless topic.is_a?(Integer)
            raise ArgumentError, 'predefined topic must be an Integer topic ID'
          end
          send_publish(:predefined, topic, data, qos, retain)
        when :short
          unless topic.is_a?(String) && topic.length == 2
            raise ArgumentError, 'short topic name must be exactly 2 characters'
          end
          send_publish(:short, topic, data, qos, retain)
        else
          raise ArgumentError, "Invalid topic type: #{topic_type}"
        end
      end

      # Handle a packet received from the UDP transport layer
      def handle_packet(packet)
        case packet
        when MQTT::SN::Packet::Advertise, MQTT::SN::Packet::Gwinfo
          handle_gateway_announce(packet)
        when MQTT::SN::Packet::Connack
          handle_connack(packet)
        when MQTT::SN::Packet::Register
          handle_register(packet)
        when MQTT::SN::Packet::Regack
          handle_regack(packet)
        when MQTT::SN::Packet::Publish
          handle_publish(packet)
        when MQTT::SN::Packet::Puback
          handle_puback(packet)
        when MQTT::SN::Packet::Disconnect
          handle_disconnect(packet)
        when MQTT::SN::Packet::Pingresp
          nil # Keep-alive response, nothing to do
        end
      end

      private

      def transmit(packet)
        @transport.send_data(packet.to_s)
        packet
      end

      def ensure_connected
        unless connected?
          raise MQTT::NotConnectedException, "Not connected to a gateway (state: #{@state})"
        end
      end

      def reset_status
        @status = :ok
        @status_message = nil
        @last_error = nil
      end

      def mark_unsupported(message)
        @status = :unsupported
        @status_message = message
        nil
      end

      # Clear all gateway-scoped state: registrations are only valid
      # for the gateway that issued them
      def reset_gateway_state
        @registrations.clear
        @pending_registrations.clear
        @pending_pubacks.clear
        @queued_publishes.clear
      end

      def next_message_id
        id = @next_message_id
        @next_message_id += 1
        @next_message_id = 1 if @next_message_id > 0xFFFF
        id
      end

      def generate_client_id
        "ruby-sn-#{rand(0x100000000).to_s(16)}"
      end

      def publish_normal(topic_name, data, qos, retain)
        topic_id = @registrations[topic_name]
        if topic_id.nil?
          @queued_publishes << [topic_name, data, qos, retain]
          register(topic_name)
          return nil
        end
        send_publish(:normal, topic_id, data, qos, retain)
      end

      def send_publish(topic_type, topic_id, data, qos, retain)
        message_id = qos == 1 ? next_message_id : 0x0000
        packet = MQTT::SN::Packet::Publish.new(
          :topic_id_type => topic_type,
          :topic_id => topic_id,
          :id => message_id,
          :qos => qos,
          :retain => retain,
          :data => data
        )
        @pending_pubacks[message_id] = packet if qos == 1
        transmit(packet)
        message_id
      end

      def flush_queued_publishes(topic_name)
        queued, @queued_publishes = @queued_publishes.partition { |entry| entry[0] == topic_name }
        queued.each do |(_name, data, qos, retain)|
          publish_normal(topic_name, data, qos, retain)
        end
      end

      def handle_gateway_announce(packet)
        duration = packet.respond_to?(:duration) ? packet.duration : nil
        @gateways[packet.gateway_id] = duration
        if @state == :discovering
          @gateway_id = packet.gateway_id
          @state = :disconnected
        end
      end

      def handle_connack(packet)
        return unless @state == :connecting

        if packet.return_code == 0x00
          @state = :connected
        else
          @last_error = packet.return_msg
          @state = :disconnected
        end
      end

      def handle_register(packet)
        @registrations[packet.topic_name] = packet.topic_id
        transmit(
          MQTT::SN::Packet::Regack.new(
            :id => packet.id,
            :topic_id => packet.topic_id,
            :return_code => 0x00
          )
        )
      end

      def handle_regack(packet)
        topic_name = @pending_registrations.delete(packet.id)
        return if topic_name.nil?

        if packet.return_code == 0x00
          @registrations[topic_name] = packet.topic_id
          flush_queued_publishes(topic_name)
        else
          @last_error = "Registration of '#{topic_name}' rejected: return code #{packet.return_code}"
          @queued_publishes.reject! { |entry| entry[0] == topic_name }
        end
      end

      def handle_publish(packet)
        if packet.qos == -1
          mark_unsupported('QoS -1 publish packets are not supported')
        elsif packet.qos == 1
          topic_id = packet.topic_id.is_a?(Integer) ? packet.topic_id : 0x0000
          transmit(
            MQTT::SN::Packet::Puback.new(
              :topic_id => topic_id,
              :id => packet.id,
              :return_code => 0x00
            )
          )
        end
      end

      def handle_puback(packet)
        @pending_pubacks.delete(packet.id)
        if packet.return_code != 0x00
          @last_error = "Publish rejected: return code #{packet.return_code}"
        end
      end

      def handle_disconnect(packet)
        if packet.duration
          mark_unsupported('Sleeping clients are not supported')
        else
          @state = :offline
        end
      end
    end
  end
end

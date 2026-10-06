module MQTT
  module SN
    # Client-side Will session logic for MQTT-SN.
    #
    # Drives the Will negotiation that happens between CONNECT and CONNACK
    # (WILLTOPICREQ/WILLTOPIC, WILLMSGREQ/WILLMSG) and the post-connect
    # Will update procedure (WILLTOPICUPD/WILLTOPICRESP,
    # WILLMSGUPD/WILLMSGRESP).
    #
    # The transport is abstracted as two callbacks, so the session can be
    # embedded in any client:
    #
    # * +send_packet+ - called with a MQTT::SN::Packet to transmit
    # * +recv_packet+ - called with a timeout in seconds, must return the
    #   next received MQTT::SN::Packet, or nil if none arrived in time
    class WillSession
      # Default number of seconds to wait for a gateway response
      DEFAULT_TIMEOUT = 10

      # Default number of times a request is retransmitted before giving up
      DEFAULT_MAX_RETRIES = 3

      attr_reader :topic, :message, :qos, :retain
      attr_accessor :timeout, :max_retries

      # Outcome of an update or clear operation. Each phase reports the
      # status that was actually confirmed by the gateway:
      # :accepted, :rejected, :timeout or :skipped (not attempted).
      class UpdateResult
        attr_reader :topic_status, :message_status

        def initialize(args = {})
          @topic_status = args.fetch(:topic_status, :skipped)
          @message_status = args.fetch(:message_status, :skipped)
        end

        # True only when every attempted phase was confirmed as accepted
        def success?
          [topic_status, message_status].all? do |status|
            status == :accepted || status == :skipped
          end
        end
      end

      def initialize(args = {})
        @topic = args[:topic]
        @message = args[:message]
        @qos = args[:qos] || 0
        @retain = args[:retain] || false
        @timeout = args[:timeout] || DEFAULT_TIMEOUT
        @max_retries = args[:max_retries] || DEFAULT_MAX_RETRIES
        @send_packet = args[:send_packet]
        @recv_packet = args[:recv_packet]
        raise ArgumentError, 'a :send_packet callback is required' if @send_packet.nil?
        raise ArgumentError, 'a :recv_packet callback is required' if @recv_packet.nil?
        @connected = false
      end

      def connected?
        @connected
      end

      # True when a Will topic has been configured
      def will?
        !@topic.nil? && !@topic.empty?
      end

      # Send CONNECT and perform the Will negotiation with the gateway:
      # WILLTOPICREQ -> WILLTOPIC, WILLMSGREQ -> WILLMSG, then CONNACK.
      # Duplicate requests from the gateway are answered by retransmitting
      # the Will data with the DUP flag set. Returns the Connack packet.
      def connect(args = {})
        raise MQTT::SN::ProtocolException, 'Already connected' if @connected

        connect_packet = MQTT::SN::Packet::Connect.new(
          :client_id => args[:client_id] || '',
          :keep_alive => args[:keep_alive] || 15,
          :clean_session => args.fetch(:clean_session, true),
          :request_will => will?
        )
        transmit(connect_packet)

        sent_topic = false
        sent_message = false
        attempts = 0
        loop do
          packet = receive
          if packet.nil?
            attempts += 1
            raise MQTT::SN::ProtocolException, 'Timed out during Will negotiation' if attempts > max_retries
            # Retransmit the last packet sent
            if sent_message
              transmit(willmsg_packet, :duplicate => true)
            elsif sent_topic
              transmit(willtopic_packet, :duplicate => true)
            else
              transmit(connect_packet, :duplicate => true)
            end
          elsif packet.is_a?(MQTT::SN::Packet::Willtopicreq) && will?
            transmit(willtopic_packet, :duplicate => sent_topic)
            sent_topic = true
          elsif packet.is_a?(MQTT::SN::Packet::Willmsgreq) && will?
            transmit(willmsg_packet, :duplicate => sent_message)
            sent_message = true
          elsif packet.is_a?(MQTT::SN::Packet::Connack)
            @connected = (packet.return_code == 0x00)
            return packet
          end
          # Anything else is stale or unrelated: discard it
        end
      end

      # Update the Will after connecting. The topic (including QoS/retain
      # flags) and the message are confirmed separately by the gateway;
      # the returned UpdateResult reports the status that was actually
      # confirmed for each phase. If the topic phase fails, the message
      # phase is skipped rather than reported as updated.
      def update(args = {})
        raise MQTT::NotConnectedException, 'Not connected' unless @connected

        new_topic = args.fetch(:topic, :unchanged)
        new_message = args.fetch(:message, :unchanged)

        topic_status = :skipped
        message_status = :skipped

        if new_topic != :unchanged || !args[:qos].nil? || !args[:retain].nil?
          upd = MQTT::SN::Packet::Willtopicupd.new(
            :topic_name => new_topic == :unchanged ? @topic : new_topic,
            :qos => args[:qos].nil? ? @qos : args[:qos],
            :retain => args[:retain].nil? ? @retain : args[:retain]
          )
          topic_status = run_phase(upd, MQTT::SN::Packet::Willtopicresp)
          unless topic_status == :accepted
            return UpdateResult.new(:topic_status => topic_status, :message_status => :skipped)
          end
          @topic = upd.topic_name
          @qos = upd.qos
          @retain = upd.retain
        end

        if new_message != :unchanged
          upd = MQTT::SN::Packet::Willmsgupd.new(:data => new_message.to_s)
          message_status = run_phase(upd, MQTT::SN::Packet::Willmsgresp)
          @message = new_message if message_status == :accepted
        end

        UpdateResult.new(:topic_status => topic_status, :message_status => message_status)
      end

      # Delete the Will stored at the gateway by sending an empty
      # WILLTOPICUPD. No message phase is attempted.
      def clear
        raise MQTT::NotConnectedException, 'Not connected' unless @connected

        status = run_phase(
          MQTT::SN::Packet::Willtopicupd.new(:topic_name => nil),
          MQTT::SN::Packet::Willtopicresp
        )
        if status == :accepted
          @topic = nil
          @message = nil
        end
        UpdateResult.new(:topic_status => status, :message_status => :skipped)
      end

      private

      # Send a request and wait for its matching response, retransmitting
      # with the DUP flag set on timeout. Packets of any other type
      # (including late confirmations from a previous round) are discarded,
      # so a stale response can never satisfy the current request.
      def run_phase(request, response_class)
        transmit(request)
        attempts = 0
        loop do
          packet = receive
          if packet.nil?
            attempts += 1
            return :timeout if attempts > max_retries
            transmit(request, :duplicate => true)
          elsif packet.is_a?(response_class)
            return packet.return_code == 0x00 ? :accepted : :rejected
          end
        end
      end

      def transmit(packet, args = {})
        packet.duplicate = args.fetch(:duplicate, false)
        @send_packet.call(packet)
      end

      def receive
        @recv_packet.call(timeout)
      end

      def willtopic_packet
        MQTT::SN::Packet::Willtopic.new(
          :topic_name => @topic,
          :qos => @qos,
          :retain => @retain
        )
      end

      def willmsg_packet
        MQTT::SN::Packet::Willmsg.new(:data => @message.to_s)
      end
    end
  end
end

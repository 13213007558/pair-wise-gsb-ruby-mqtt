# encoding: BINARY

require 'timeout'

module MQTT
  module SN
    class WillSession
      Will = Struct.new(:topic, :message, :qos, :retain, :keyword_init => true)

      Acknowledgement = Struct.new(
        :phase, :state, :return_code, :attempts, :keyword_init => true
      ) do
        def accepted?
          state == :accepted
        end

        def rejected?
          state == :rejected
        end

        def timed_out?
          state == :timeout
        end

        def skipped?
          state == :skipped
        end
      end

      Result = Struct.new(
        :connect_ack, :topic_ack, :message_ack, :will, :keyword_init => true
      ) do
        def accepted?
          acks.all?(&:accepted?)
        end

        def rejected?
          acks.any?(&:rejected?)
        end

        def timed_out?
          acks.any?(&:timed_out?)
        end

        private

        def acks
          [connect_ack, topic_ack, message_ack].compact.reject(&:skipped?)
        end
      end

      attr_reader :will, :last_result, :epoch, :pending_phase

      def initialize(send_packet:, receive_packet: nil, unhandled_packet: nil,
                     ack_timeout: 5, retries: 2)
        raise ArgumentError, 'send_packet callback is required' unless send_packet.respond_to?(:call)
        if receive_packet && !receive_packet.respond_to?(:call)
          raise ArgumentError, 'receive_packet must respond to call'
        end
        if unhandled_packet && !unhandled_packet.respond_to?(:call)
          raise ArgumentError, 'unhandled_packet must respond to call'
        end

        @send_packet = send_packet
        @receive_packet = receive_packet
        @unhandled_packet = unhandled_packet
        @ack_timeout = ack_timeout
        @retries = retries
        @will = nil
        @connected = false
        @epoch = 0
        @pending_phase = nil
        @pending_expected = []
        @pending_generation = nil
        @events = []
        @event_mutex = Mutex.new
        @event_condition = ConditionVariable.new
      end

      def connected?
        @connected
      end

      def connect(client_id:, topic: nil, message: '', qos: 0, retain: false,
                  keep_alive: 15, clean_session: true)
        will = topic.nil? ? nil : build_will(topic, message, qos, retain)

        packet = Packet::Connect.new(
          :client_id => client_id,
          :keep_alive => keep_alive,
          :clean_session => clean_session,
          :request_will => !will.nil?
        )

        begin_pending(:connect, connect_expected(will))
        transmit(packet)

        return finish_connect(receive_connect_packet([Packet::Connack]), nil) if will.nil?

        negotiate_after_connect(will)
      end

      def negotiate(topic:, message: '', qos: 0, retain: false)
        will = build_will(topic, message, qos, retain)
        begin_pending(:connect, [Packet::Willtopicreq])
        negotiate_after_connect(will)
      end

      def replace_will(topic:, message:, qos: 0, retain: false)
        require_connected
        desired = build_will(topic, message, qos, retain)
        topic_ack = update_topic_if_needed(desired)
        return update_result(topic_ack, skipped_ack(:message)) unless topic_ack.accepted?

        update_result(topic_ack, update_message_if_needed(desired))
      end
      alias update_will replace_will
      alias set_will replace_will

      def update_will_topic(topic:, qos: @will&.qos || 0,
                            retain: @will&.retain || false)
        require_connected
        desired = build_will(topic, @will&.message || '', qos, retain)
        if will_topic?(@will, desired)
          return update_result(skipped_ack(:topic), skipped_ack(:message))
        end

        update_result(request_topic_update(desired), skipped_ack(:message))
      end
      alias update_topic update_will_topic

      def update_will_message(message)
        require_connected
        raise ArgumentError, 'message must be a String' unless message.is_a?(String)
        raise ArgumentError, 'cannot update a message without a will topic' if @will.nil?

        desired = @will.dup
        desired.message = message
        if @will.message == message
          return update_result(skipped_ack(:topic), skipped_ack(:message))
        end

        update_result(skipped_ack(:topic), request_message_update(desired))
      end
      alias update_message update_will_message

      def clear_will
        require_connected
        ack = request_ack(
          :topic,
          Packet::Willtopicupd.new(:topic_name => nil),
          Packet::Willtopicresp
        )

        @will = nil if ack.accepted?
        update_result(ack, skipped_ack(:message))
      end
      alias clear clear_will
      alias negotiate_will negotiate

      def handle_packet(packet)
        packet = coerce_packet(packet)
        accepted = @event_mutex.synchronize do
          if @pending_expected.include?(packet.class)
            @events << packet
            @event_condition.signal
            true
          end
        end
        return :accepted if accepted
        return :ignored if will_packet?(packet)
        if @unhandled_packet
          @unhandled_packet.call(packet)
          return :unhandled
        end

        :ignored
      end

      private

      def negotiate_after_connect(will)
        stage = :topic

        loop do
          set_pending_expected(connect_stage_expected(stage))
          packet = receive_connect_packet(connect_stage_expected(stage))

          case packet
          when Packet::Willtopicreq
            transmit(build_will_topic(Packet::Willtopic, will))
            stage = :message
          when Packet::Willmsgreq
            transmit(Packet::Willmsg.new(:data => will.message))
            stage = :connack
          when Packet::Connack
            return finish_connect(packet, will)
          end
        end
      end

      def connect_expected(will)
        will.nil? ? [Packet::Connack] : [Packet::Willtopicreq]
      end

      def connect_stage_expected(stage)
        case stage
        when :topic
          [Packet::Willtopicreq]
        when :message
          [Packet::Willtopicreq, Packet::Willmsgreq]
        else
          [Packet::Willtopicreq, Packet::Willmsgreq, Packet::Connack]
        end
      end

      def finish_connect(packet, will)
        accepted = packet.return_code.zero?
        ack = acknowledgement(:connect, accepted ? :accepted : :rejected, packet.return_code, 1)
        @connected = accepted
        @will = accepted ? will : nil
        clear_pending
        @last_result = Result.new(:connect_ack => ack, :will => @will)
      end

      def update_topic_if_needed(desired)
        return skipped_ack(:topic) if will_topic?(@will, desired)

        request_topic_update(desired)
      end

      def update_message_if_needed(desired)
        return skipped_ack(:message) if @will && @will.message == desired.message

        request_message_update(desired)
      end

      def request_topic_update(desired)
        ack = request_ack(
          :topic,
          build_will_topic(Packet::Willtopicupd, desired),
          Packet::Willtopicresp
        )
        apply_topic_update(desired) if ack.accepted?
        ack
      end

      def request_message_update(desired)
        ack = request_ack(
          :message,
          Packet::Willmsgupd.new(:data => desired.message),
          Packet::Willmsgresp
        )
        @will = desired.dup if ack.accepted?
        ack
      end

      def request_ack(phase, packet, response_class)
        generation = begin_pending(phase, [response_class])
        attempts = 0

        begin
          packet.duplicate = attempts.zero? ? false : true
          transmit(packet)
          response = receive_expected(response_class, phase, generation)
          state = response.return_code.zero? ? :accepted : :rejected
          acknowledgement(phase, state, response.return_code, attempts + 1)
        rescue Timeout::Error
          attempts += 1
          retry if attempts <= @retries
          acknowledgement(phase, :timeout, nil, attempts)
        ensure
          clear_pending(generation)
        end
      end

      def receive_expected(response_class, phase, generation)
        loop do
          packet = read_packet(@ack_timeout)
          next unless current_pending?(phase, generation)
          return packet if packet.class == response_class

          dispatch_unexpected(packet) unless will_packet?(packet)
        end
      end

      def receive_connect_packet(expected)
        loop do
          packet = read_packet(nil)
          return packet if expected.include?(packet.class)

          if packet.is_a?(Packet)
            raise ProtocolException, "Unexpected MQTT-SN packet during will negotiation: #{packet.class}"
          else
            dispatch_unexpected(packet)
          end
        end
      end

      def read_packet(timeout)
        if @receive_packet
          if timeout.nil?
            packet = @receive_packet.call
          else
            packet = Timeout.timeout(timeout) { @receive_packet.call }
          end
          return coerce_packet(packet)
        end

        wait_for_event(timeout)
      end

      def wait_for_event(timeout)
        @event_mutex.synchronize do
          if timeout.nil?
            @event_condition.wait(@event_mutex) while @events.empty?
          else
            deadline = current_time + timeout
            while @events.empty?
              remaining = deadline - current_time
              raise Timeout::Error if remaining <= 0

              @event_condition.wait(@event_mutex, remaining)
            end
          end
          @events.shift
        end
      end

      def dispatch_unexpected(packet)
        return :ignored unless @unhandled_packet

        @unhandled_packet.call(packet)
        :unhandled
      end

      def apply_topic_update(desired)
        @will = Will.new(
          :topic => desired.topic,
          :message => @will.nil? ? '' : @will.message,
          :qos => desired.qos,
          :retain => desired.retain
        )
      end

      def build_will_topic(klass, will)
        klass.new(
          :topic_name => will.topic,
          :qos => will.qos,
          :retain => will.retain
        )
      end

      def build_will(topic, message, qos, retain)
        raise ArgumentError, 'topic must be a String' unless topic.is_a?(String)
        raise ArgumentError, 'topic cannot be empty' if topic.empty?
        raise ArgumentError, 'message must be a String' unless message.is_a?(String)
        raise ArgumentError, 'qos must be 0, 1 or 2' unless [0, 1, 2].include?(qos)
        raise ArgumentError, 'retain must be true or false' unless retain == true || retain == false

        Will.new(
          :topic => topic,
          :message => message,
          :qos => qos,
          :retain => retain
        )
      end

      def will_topic?(current, desired)
        !current.nil? &&
          current.topic == desired.topic &&
          current.qos == desired.qos &&
          current.retain == desired.retain
      end

      def will_packet?(packet)
        [
          Packet::Willtopicreq,
          Packet::Willtopic,
          Packet::Willmsgreq,
          Packet::Willmsg,
          Packet::Willtopicupd,
          Packet::Willtopicresp,
          Packet::Willmsgupd,
          Packet::Willmsgresp
        ].include?(packet.class)
      end

      def transmit(packet)
        @send_packet.call(packet.to_s)
      end

      def coerce_packet(packet)
        return packet if packet.is_a?(Packet)

        Packet.parse(packet)
      end

      def require_connected
        raise ProtocolException, 'Will session is not connected' unless connected?
      end

      def skipped_ack(phase)
        Acknowledgement.new(
          :phase => phase,
          :state => :skipped,
          :return_code => nil,
          :attempts => 0
        )
      end

      def acknowledgement(phase, state, return_code, attempts)
        Acknowledgement.new(
          :phase => phase,
          :state => state,
          :return_code => return_code,
          :attempts => attempts
        )
      end

      def update_result(topic_ack, message_ack)
        @last_result = Result.new(
          :topic_ack => topic_ack,
          :message_ack => message_ack,
          :will => @will
        )
      end

      def begin_pending(phase, expected)
        @epoch += 1
        @pending_phase = phase
        @pending_expected = expected
        @pending_generation = @epoch
        @epoch
      end

      def set_pending_expected(expected)
        @pending_expected = expected
      end

      def clear_pending(generation = nil)
        return unless generation.nil? || generation == @pending_generation

        @pending_phase = nil
        @pending_expected = []
        @pending_generation = nil
      end

      def current_pending?(phase, generation)
        @pending_phase == phase && @pending_generation == generation
      end

      def current_time
        Process.clock_gettime(Process::CLOCK_MONOTONIC)
      end
    end
  end
end

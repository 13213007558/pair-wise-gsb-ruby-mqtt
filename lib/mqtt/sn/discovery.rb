# encoding: BINARY

require 'mqtt/sn/packet'

module MQTT
  module SN
    # Standalone MQTT-SN gateway discovery component.
    #
    # Processes SEARCHGW, GWINFO and ADVERTISE UDP datagrams, keeps a bounded,
    # time-limited set of candidate gateways and tells the caller which
    # gateways can currently be selected. It never performs real network I/O
    # itself: packets to send are returned to the caller and time is supplied
    # through an injectable clock, which makes the behaviour deterministic
    # and testable.
    #
    # Information reported directly by a gateway (an ADVERTISE, or a GWINFO
    # whose embedded address matches its sender) is trusted more than
    # information relayed by another client. Candidate expiry only removes a
    # gateway from the set of choices; this component never declares an
    # established MQTT session to be dead.
    class Discovery
      # Address used as the destination of outbound broadcast datagrams
      BROADCAST_ADDRESS = :broadcast

      # A single candidate gateway and the period for which it is valid
      class Gateway
        attr_reader :gateway_id
        attr_reader :address
        attr_reader :first_seen
        attr_reader :last_seen
        attr_reader :expires_at

        def initialize(gateway_id, address, expires_at, first_seen, options = {})
          @gateway_id = gateway_id
          @address = address
          @expires_at = expires_at
          @first_seen = first_seen
          @last_seen = options[:last_seen] || first_seen
          @relay = options[:relay]
        end

        # True when the information came directly from the gateway itself
        def direct?
          @relay.nil?
        end

        # Address of the client that relayed the information, or nil
        def forwarded_by
          @relay
        end

        def expired?(now)
          now >= @expires_at
        end

        def remaining(now)
          @expires_at - now
        end

        # Apply a renewal from the gateway itself
        def renew_direct(gateway_id, now, expires_at)
          @gateway_id = gateway_id
          @relay = nil
          update(now, expires_at)
        end

        # Apply a renewal reported by another client
        def renew_forwarded(gateway_id, relay, now, expires_at)
          # Direct information and direct gateway ids are never downgraded by
          # a relayed claim; it may only extend validity. A candidate that is
          # already relayed adopts the newest reporter and id.
          if @relay
            @gateway_id = gateway_id
            @relay = relay
          end
          update(now, expires_at)
        end

        # Promote relayed information once the gateway speaks for itself
        def promote(gateway_id, now, expires_at)
          @gateway_id = gateway_id
          @relay = nil
          update(now, expires_at)
        end

        def rank_key(now)
          [
            direct? ? 0 : 1,
            -remaining(now),
            @gateway_id,
            @address.to_s
          ]
        end

        private

        def update(now, expires_at)
          @last_seen = now
          @expires_at = expires_at
        end
      end

      DEFAULTS = {
        # Radius placed in SEARCHGW packets we originate
        :radius => 1,
        # Validity (seconds) assigned to candidates learned from GWINFO,
        # which carries no duration field
        :gateway_ttl => 900,
        # Maximum number of candidate gateways that are retained
        :max_candidates => 8,
        # Maximum number of delayed SEARCHGW replies kept pending at once
        :max_pending_replies => 16,
        # Minimum gap between SEARCHGW broadcasts we originate
        :min_search_interval => 10,
        # Minimum gap between SEARCHGW broadcasts we forward
        :min_forward_interval => 10,
        # How long a forwarded SEARCHGW is remembered for de-duplication
        :request_window => 30,
        # Random delay (seconds) before answering a SEARCHGW with GWINFO
        :response_delay => 1..5,
        # Fixed delay (seconds) before forwarding a SEARCHGW
        :forward_delay => 1
      }

      attr_reader :candidates

      def initialize(options = {})
        config = DEFAULTS.merge(options)
        @radius = config[:radius]
        @gateway_ttl = config[:gateway_ttl]
        @max_candidates = config[:max_candidates]
        @max_pending_replies = config[:max_pending_replies]
        @min_search_interval = config[:min_search_interval]
        @min_forward_interval = config[:min_forward_interval]
        @request_window = config[:request_window]
        @response_delay = config[:response_delay]
        @forward_delay = config[:forward_delay]
        @clock = config[:clock] || lambda { Time.now.to_i }
        @random = config[:random] || Random.new

        @candidates = {}
        @seen_requests = {}
        @pending = []
        @sequence = 0
        @last_search_at = nil
        @last_forward_at = nil
        @pruned = []
      end

      # Candidate gateways that are currently valid, best choice first
      def gateways(now = @clock.call)
        live = @candidates.values.reject { |gateway| gateway.expired?(now) }
        live.sort_by { |gateway| gateway.rank_key(now) }
      end

      # Candidates removed by the most recent expiry sweep
      def pruned_gateways
        @pruned.dup
      end

      # Current best gateway, or nil when no candidate is valid. A nil result
      # only means "no choice available"; it says nothing about any existing
      # MQTT session.
      def select_gateway(now = @clock.call)
        gateways(now).first
      end

      # Originate a SEARCHGW broadcast. Returns the datagram, or nil when the
      # send-frequency limit suppresses the request.
      def search!
        now = @clock.call
        if @last_search_at && (now - @last_search_at) < @min_search_interval
          return nil
        end

        @last_search_at = now
        packet = MQTT::SN::Packet::Searchgw.new(:radius => @radius)
        [packet.to_s, BROADCAST_ADDRESS]
      end

      # Process one inbound UDP datagram. Any datagrams that must be sent
      # immediately are returned as [payload, destination] pairs; delayed
      # responses and forwards are emitted later by #tick.
      def receive(data, source)
        now = @clock.call
        packet = MQTT::SN::Packet.parse(data)
        case packet
        when MQTT::SN::Packet::Searchgw
          receive_searchgw(packet, source, now)
        when MQTT::SN::Packet::Gwinfo
          receive_gwinfo(packet, source, now)
        when MQTT::SN::Packet::Advertise
          receive_advertise(packet, source, now)
        else
          []
        end
      end

      # Advance the fake clock: expire candidate gateways, forget old requests
      # and emit any delayed datagrams that have become due.
      def tick(now = @clock.call)
        expire(now)
        sweep_requests(now)
        due = []

        while (action = @pending.first) && action[:at] <= now
          @pending.shift
          datagram = action[:call].call(now)
          due << datagram if datagram
        end

        due
      end

      private

      def receive_searchgw(packet, source, now)
        key = request_key(packet, source)
        # Only the first appearance from a searcher within the window is
        # acted upon: repeats neither reset the window nor trigger replies or
        # further forwards.
        return [] if @seen_requests.key?(key)

        @seen_requests[key] = now + @request_window

        # Reply with our chosen gateway after a randomised delay, so a crowd of
        # clients does not answer simultaneously. Only one reply per searcher
        # is ever queued.
        schedule_reply(source, now) unless pending_reply?(source)

        # Forward with a radius reduced by one hop. Requests that have reached
        # radius 1 are never rebroadcast.
        radius = packet.radius.to_i
        if radius > 1 && can_forward?(now)
          schedule_forward(radius - 1, now)
        end

        []
      end

      def receive_gwinfo(packet, source, now)
        address = packet.gateway_address
        address = source if address.nil? || address.empty?

        direct = address == source
        expires_at = now + @gateway_ttl

        remember_request_answer(packet.gateway_id)
        upsert(address) do |gateway|
          if gateway.nil?
            if direct
              direct_gateway(packet.gateway_id, address, now, expires_at)
            else
              forwarded_gateway(packet.gateway_id, address, source, now, expires_at)
            end
          elsif direct
            gateway.promote(packet.gateway_id, now, expires_at)
            gateway
          else
            gateway.renew_forwarded(packet.gateway_id, source, now, expires_at)
            gateway
          end
        end
        []
      end

      def receive_advertise(packet, source, now)
        expires_at = now + packet.duration.to_i

        if packet.duration.to_i <= 0
          # A zero duration withdraws the gateway immediately.
          removed = @candidates.delete(source)
          @pruned << removed if removed
          return []
        end

        upsert(source) do |gateway|
          if gateway
            gateway.renew_direct(packet.gateway_id, now, expires_at)
            gateway
          else
            direct_gateway(packet.gateway_id, source, now, expires_at)
          end
        end
        []
      end

      def direct_gateway(gateway_id, address, now, expires_at)
        Gateway.new(gateway_id, address, expires_at, now)
      end

      def forwarded_gateway(gateway_id, address, relay, now, expires_at)
        Gateway.new(gateway_id, address, expires_at, now, :relay => relay)
      end

      # Insert or replace a candidate while honouring the candidate count
      # limit. A better-ranked newcomer may evict the current worst gateway;
      # otherwise it is rejected.
      def upsert(address)
        existing = @candidates[address]
        if existing
          yield(existing)
        elsif @candidates.size < @max_candidates
          @candidates[address] = yield(nil)
        else
          now = @clock.call
          worst = gateways(now).last
          candidate = yield(nil)
          if worst && (candidate.rank_key(now) <=> worst.rank_key(now)) < 0
            @candidates.delete(worst.address)
            @candidates[address] = candidate
          end
        end
      end

      def remember_request_answer(gateway_id)
        # Suppress our own delayed reply once an answer for the same gateway
        # has been observed on the wire.
        @pending.delete_if do |action|
          action[:type] == :reply && action[:gateway_id] == gateway_id
        end
      end

      def request_key(packet, source)
        [:seen, source, packet.radius.to_i]
      end

      def sweep_requests(now)
        @seen_requests.delete_if { |_key, until_at| until_at <= now }
      end

      def can_forward?(now)
        @last_forward_at.nil? || (now - @last_forward_at) >= @min_forward_interval
      end

      def schedule_forward(radius, now)
        @last_forward_at = now
        schedule(now + @forward_delay) do
          packet = MQTT::SN::Packet::Searchgw.new(:radius => radius)
          [packet.to_s, BROADCAST_ADDRESS]
        end
      end

      def pending_reply?(source)
        @pending.any? { |action| action[:type] == :reply && action[:source] == source }
      end

      def schedule_reply(source, now)
        pending_replies = @pending.count { |action| action[:type] == :reply }
        return if pending_replies >= @max_pending_replies
        gateway = select_gateway(now)
        return unless gateway

        delay = @random.rand(@response_delay)
        action = {
          :type => :reply,
          :source => source,
          :gateway_id => gateway.gateway_id,
          :at => now + delay,
          :sequence => next_sequence,
          :call => lambda { |fire_at| build_reply(source, gateway, fire_at) }
        }
        insert_sorted(action)
      end

      def build_reply(source, gateway, now)
        # The gateway may have expired or changed during the randomised wait.
        current = @candidates[gateway.address]
        return nil unless current
        return nil if current.gateway_id != gateway.gateway_id
        return nil if current.expired?(now)

        packet = MQTT::SN::Packet::Gwinfo.new(
          :gateway_id => gateway.gateway_id,
          :gateway_address => gateway.address
        )
        [packet.to_s, source]
      end

      def schedule(at, &block)
        action = {
          :type => :other,
          :at => at,
          :sequence => next_sequence,
          :call => block
        }
        insert_sorted(action)
      end

      def insert_sorted(action)
        index = @pending.index { |other| due_before?(action, other) }
        if index
          @pending.insert(index, action)
        else
          @pending << action
        end
      end

      def due_before?(action, other)
        if action[:at] == other[:at]
          action[:sequence] < other[:sequence]
        else
          action[:at] < other[:at]
        end
      end

      def next_sequence
        @sequence += 1
      end

      def expire(now)
        expired = @candidates.values.select { |gateway| gateway.expired?(now) }
        expired.each { |gateway| @candidates.delete(gateway.address) }
        @pruned = expired.sort_by { |gateway| gateway.rank_key(now) }
      end
    end
  end
end

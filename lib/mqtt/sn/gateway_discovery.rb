# encoding: BINARY

module MQTT
  module SN
    # Gateway discovery for MQTT-SN clients that do not know the address of
    # a gateway in advance.
    #
    # The component handles the three discovery related packet types:
    #
    # * ADVERTISE - broadcast periodically by gateways
    # * SEARCHGW  - broadcast by the client when it looks for gateways
    # * GWINFO    - reply to SEARCHGW, sent either by a gateway itself or
    #   forwarded by another client
    #
    # It is deliberately passive: it never opens sockets and never decides
    # that an MQTT session is dead. It only maintains a bounded list of
    # gateway candidates with expiry times, so the caller can pick one.
    # When every candidate has expired, +candidates+ is simply empty and
    # +best_gateway+ returns nil; what that means for the session is left
    # to the caller.
    #
    # Deterministic rules:
    #
    # * Information learned directly from a gateway (ADVERTISE, or GWINFO
    #   sent by the gateway itself) is trusted more than information
    #   forwarded by another client (GWINFO whose payload address differs
    #   from the datagram source).
    # * A GWINFO is only accepted within +response_window+ seconds after
    #   the last SEARCHGW was sent; later replies are dropped as delayed.
    # * If two different addresses claim the same gateway id, the more
    #   trusted source wins; on equal trust the newest information wins.
    #   Every conflict is recorded in +conflicts+.
    # * A gateway id seen again at the same address is a renewal: only the
    #   expiry time is updated.
    # * The candidate list never exceeds +max_candidates+; when full, the
    #   weakest candidate (client-forwarded first, then earliest expiry)
    #   is evicted. A forwarded candidate is never admitted at the cost of
    #   a gateway-confirmed one.
    # * SEARCHGW requests are rate limited to one per +min_search_interval+
    #   and the radius is clamped to +max_radius+, so repeated incoming
    #   broadcasts can never amplify outgoing traffic. Receiving datagrams
    #   never triggers any transmission.
    class GatewayDiscovery
      # Trust ranking of the two information sources
      SOURCE_TRUST = {
        :client => 0,  # forwarded by another client (GWINFO with GwAdd)
        :gateway => 1  # learned from the gateway itself (ADVERTISE/GWINFO)
      }

      DEFAULT_MAX_RADIUS = 3
      DEFAULT_RESPONSE_WINDOW = 2.0
      DEFAULT_MIN_SEARCH_INTERVAL = 5.0
      DEFAULT_MAX_CANDIDATES = 8
      # GWINFO carries no duration, so candidates learned from it get this
      # fallback lifetime (seconds)
      DEFAULT_GWINFO_DURATION = 60

      # A single discovered gateway with its expiry time
      class Candidate
        attr_reader :gateway_id     # id advertised by the gateway
        attr_reader :address        # where the gateway can be reached
        attr_reader :source         # :gateway (direct) or :client (forwarded)
        attr_reader :discovered_at  # clock value when first learned
        attr_reader :updated_at     # clock value of last confirmation
        attr_reader :expires_at     # clock value after which it is stale

        def initialize(gateway_id, address, source, expires_at, now)
          @gateway_id = gateway_id
          @address = address
          @source = source
          @expires_at = expires_at
          @discovered_at = now
          @updated_at = now
        end

        # Seconds of validity left; negative once expired
        def ttl(now)
          @expires_at - now
        end

        def expired?(now)
          now >= @expires_at
        end

        # True when learned directly from the gateway
        def gateway?
          @source == :gateway
        end

        # Refresh an existing candidate seen again at the same address.
        # A direct sighting upgrades a previously forwarded candidate.
        def renew(source, expires_at, now)
          if SOURCE_TRUST[source] > SOURCE_TRUST[@source]
            @source = source
          end
          @expires_at = expires_at
          @updated_at = now
        end

        # Move the candidate to a different address after a conflict
        def relocate(address, source, expires_at, now)
          @address = address
          @source = source
          @expires_at = expires_at
          @updated_at = now
        end
      end

      attr_reader :max_radius, :response_window, :min_search_interval
      attr_reader :max_candidates, :gwinfo_duration, :conflicts

      # Options (all optional):
      # * :clock - callable returning monotonic seconds (injectable for tests)
      # * :max_candidates - cap on the candidate list size
      # * :max_radius - upper bound for the SEARCHGW broadcast radius
      # * :response_window - seconds after a SEARCHGW during which GWINFO
      #   replies are accepted
      # * :min_search_interval - minimum seconds between two SEARCHGW
      #   transmissions
      # * :gwinfo_duration - fallback lifetime for candidates learned via
      #   GWINFO (which carries no duration)
      def initialize(args = {})
        @clock = args[:clock] || lambda { Process.clock_gettime(Process::CLOCK_MONOTONIC) }
        @max_candidates = args.fetch(:max_candidates, DEFAULT_MAX_CANDIDATES)
        @max_radius = args.fetch(:max_radius, DEFAULT_MAX_RADIUS)
        @response_window = args.fetch(:response_window, DEFAULT_RESPONSE_WINDOW)
        @min_search_interval = args.fetch(:min_search_interval, DEFAULT_MIN_SEARCH_INTERVAL)
        @gwinfo_duration = args.fetch(:gwinfo_duration, DEFAULT_GWINFO_DURATION)
        @candidates = []
        @conflicts = []
        @last_search_at = nil
      end

      # Build a SEARCHGW datagram to broadcast, or return nil when the
      # previous search is still within +min_search_interval+. The radius
      # is clamped to the range 1..max_radius.
      def search(radius = 1)
        now = @clock.call
        if @last_search_at && (now - @last_search_at) < @min_search_interval
          return nil
        end
        @last_search_at = now
        radius = [[radius, 1].max, @max_radius].min
        MQTT::SN::Packet::Searchgw.new(:radius => radius).to_s
      end

      # Process one received UDP datagram. +from+ is the opaque source
      # address of the datagram, as reported by the caller's transport.
      # Returns a symbol describing what happened:
      #
      # * :added - a new candidate was learned
      # * :renewed - a known gateway was confirmed again at the same address
      # * :conflict - the gateway id was already known at another address
      # * :dropped - the candidate list is full of more trusted gateways
      # * :ignored_late - GWINFO arrived after the response window
      # * :ignored - anything else (other packet types, malformed data,
      #   ADVERTISE with zero duration)
      #
      # Never returns anything to transmit: receiving datagrams must not
      # amplify outgoing traffic.
      def receive_datagram(bytes, from)
        packet =
          begin
            MQTT::SN::Packet.parse(bytes)
          rescue MQTT::SN::ProtocolException
            return :ignored
          end

        case packet
        when MQTT::SN::Packet::Advertise
          handle_advertise(packet, from)
        when MQTT::SN::Packet::Gwinfo
          handle_gwinfo(packet, from)
        else
          :ignored
        end
      end

      # Live candidates, most preferred first: gateway-confirmed before
      # client-forwarded, then longest remaining validity, then address.
      def candidates
        prune
        @candidates.sort_by do |candidate|
          [-SOURCE_TRUST[candidate.source], -candidate.expires_at, candidate.address.to_s]
        end
      end

      # The currently preferred gateway candidate, or nil when none is
      # known. Nil only means "nothing to suggest", never "session dead".
      def best_gateway
        candidates.first
      end

      # Remove and return the candidates whose validity has expired
      def prune
        now = @clock.call
        expired, @candidates = @candidates.partition { |c| c.expired?(now) }
        expired
      end

      private

      def handle_advertise(packet, from)
        # A zero duration gives the candidate no validity at all
        return :ignored if packet.duration.to_i <= 0

        learn(packet.gateway_id, from, :gateway, @clock.call + packet.duration)
      end

      def handle_gwinfo(packet, from)
        now = @clock.call
        return :ignored_late unless search_pending?(now)

        if packet.gateway_address.nil? || packet.gateway_address == from
          # Sent by the gateway itself; it lives at the datagram source
          learn(packet.gateway_id, from, :gateway, now + @gwinfo_duration)
        else
          # Forwarded by another client about a gateway it knows
          learn(packet.gateway_id, packet.gateway_address, :client, now + @gwinfo_duration)
        end
      end

      def search_pending?(now)
        !@last_search_at.nil? && (now - @last_search_at) <= @response_window
      end

      def learn(gateway_id, address, source, expires_at)
        now = @clock.call
        existing = @candidates.find { |c| c.gateway_id == gateway_id }

        if existing
          return learn_conflict(existing, address, source, expires_at, now) unless existing.address == address

          existing.renew(source, expires_at, now)
          return :renewed
        end

        return :dropped unless make_room_for(source)

        @candidates << Candidate.new(gateway_id, address, source, expires_at, now)
        :added
      end

      def learn_conflict(existing, address, source, expires_at, now)
        # More trusted source wins; on equal trust the newest wins
        if SOURCE_TRUST[source] >= SOURCE_TRUST[existing.source]
          record_conflict(existing.gateway_id, address, existing.address, now)
          existing.relocate(address, source, expires_at, now)
        else
          record_conflict(existing.gateway_id, existing.address, address, now)
        end
        :conflict
      end

      def record_conflict(gateway_id, kept_address, dropped_address, now)
        @conflicts << {
          :gateway_id => gateway_id,
          :kept_address => kept_address,
          :dropped_address => dropped_address,
          :at => now
        }
      end

      # Ensure there is room for one more candidate. Returns false when the
      # list is full of candidates that are all more trusted than +source+.
      def make_room_for(source)
        prune
        return true if @candidates.length < @max_candidates

        weakest = @candidates.min_by do |candidate|
          [SOURCE_TRUST[candidate.source], candidate.expires_at]
        end
        return false if SOURCE_TRUST[weakest.source] > SOURCE_TRUST[source]

        @candidates.delete(weakest)
        true
      end
    end
  end
end

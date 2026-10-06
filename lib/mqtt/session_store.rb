# encoding: BINARY

module MQTT
  # Base class for session stores.
  #
  # A session store persists the state of in-flight QoS 2 message
  # handshakes so that a client can resume a persistent session
  # (Clean Session = 0) after a reconnect or a process restart,
  # as required by the MQTT 3.1.1 specification.
  #
  # The store keeps two separate packet identifier spaces:
  #
  # * outgoing: QoS 2 PUBLISH packets sent by this client, tracked
  #   through the PUBLISH -> PUBREC -> PUBREL -> PUBCOMP handshake.
  # * incoming: QoS 2 PUBLISH packets received from the server,
  #   tracked through the PUBLISH -> PUBREC -> PUBREL -> PUBCOMP
  #   handshake so duplicates are never delivered twice.
  #
  # Subclass this and implement the same interface as
  # MQTT::SessionStore::Memory to back the session with a
  # persistent medium (file, database, ...) that survives restarts.
  class SessionStore
    # In-memory session store.
    #
    # Keeps the session state for the lifetime of the process.
    # Inject your own subclass of MQTT::SessionStore if the state
    # needs to survive a process restart:
    #
    #   client = MQTT::Client.new(
    #     :host => "mqtt.example.com",
    #     :clean_session => false,
    #     :session_store => MyFileStore.new("session.db")
    #   )
    class Memory < SessionStore
      def initialize
        @semaphore = Mutex.new
        clear
      end

      # Discard all session state (Clean Session = 1 semantics)
      def clear
        @semaphore.synchronize do
          @last_packet_id = 0
          @outgoing = {}
          @incoming = {}
        end
      end

      # The last packet identifier used for an outgoing packet
      def last_packet_id
        @semaphore.synchronize { @last_packet_id }
      end

      # Set the last packet identifier used for an outgoing packet
      def last_packet_id=(value)
        @semaphore.synchronize { @last_packet_id = value }
      end

      # Remember an outgoing QoS 2 PUBLISH awaiting its PUBREC
      def store_outgoing(packet)
        @semaphore.synchronize { @outgoing[packet.id] = [:publish, packet] }
      end

      # Returns true if the packet id is used by an outgoing packet
      def outgoing_id?(id)
        @semaphore.synchronize { @outgoing.key?(id) }
      end

      # State of an outgoing packet id (:publish or :pubrel), or nil
      def outgoing_state(id)
        @semaphore.synchronize do
          entry = @outgoing[id]
          entry.nil? ? nil : entry.first
        end
      end

      # Note that a PUBREC was received and a PUBREL was sent
      def mark_outgoing_pubrel(id)
        @semaphore.synchronize do
          entry = @outgoing[id]
          entry[0] = :pubrel unless entry.nil?
        end
      end

      # Forget an outgoing packet once its PUBCOMP was received
      def delete_outgoing(id)
        @semaphore.synchronize { @outgoing.delete(id) }
      end

      # Yield (state, packet) for every unacknowledged outgoing
      # packet. The block must not call back into the store.
      def each_outgoing(&block)
        @semaphore.synchronize do
          @outgoing.each_value do |state, packet|
            block.call(state, packet)
          end
        end
      end

      # Remember an incoming QoS 2 PUBLISH awaiting its PUBREL
      def store_incoming(packet)
        @semaphore.synchronize { @incoming[packet.id] = [:pubrec, packet] }
      end

      # State of an incoming packet id (:pubrec or :pubcomp), or nil
      def incoming_state(id)
        @semaphore.synchronize do
          entry = @incoming[id]
          entry.nil? ? nil : entry.first
        end
      end

      # Note that a PUBREL was received and a PUBCOMP was sent.
      # Returns the stored PUBLISH packet to deliver to the
      # application, or nil if the packet id is unknown.
      def mark_incoming_pubcomp(id)
        @semaphore.synchronize do
          entry = @incoming[id]
          next nil if entry.nil?

          entry[0] = :pubcomp
          entry[1]
        end
      end
    end
  end
end

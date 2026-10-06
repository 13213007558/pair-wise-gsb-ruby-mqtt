# encoding: BINARY

module MQTT
  # Thread-safe in-memory storage for persistent MQTT client sessions
  class InMemorySessionStore
    def initialize
      @sessions = {}
      @mutex = Mutex.new
    end

    def load_session(client_id)
      @mutex.synchronize do
        session = @sessions[client_id]
        session.nil? ? nil : Marshal.load(Marshal.dump(session))
      end
    end

    def save_session(client_id, session)
      @mutex.synchronize do
        @sessions[client_id] = Marshal.load(Marshal.dump(session))
      end
    end

    def delete_session(client_id)
      @mutex.synchronize do
        @sessions.delete(client_id)
      end
    end
  end
end

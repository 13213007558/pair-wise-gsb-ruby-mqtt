require 'socket'

module MQTT
  module SN
    # Datagram transport backed by a connected UDP socket.
    #
    # Each call to #send writes one datagram and each call to #recv
    # reads one datagram, which is exactly the framing that MQTT-SN
    # uses over UDP. The socket is created lazily on first use.
    class UDPTransport
      # Maximum size of a MQTT-SN datagram that will be read
      MAX_PACKET_SIZE = 65_535

      attr_reader :host, :port

      def initialize(host, port = MQTT::SN::DEFAULT_PORT, socket: nil)
        @host = host
        @port = port
        @socket = socket
        @connected = false
      end

      # Connect the underlying UDP socket to the fixed gateway.
      def connect
        @socket ||= UDPSocket.new
        unless @connected
          @socket.connect(@host, @port)
          @connected = true
        end
        self
      end

      # Send one datagram to the fixed gateway.
      def send(data)
        connect
        @socket.send(data, 0)
      end

      # Wait up to +timeout+ seconds for one datagram from the gateway.
      # Returns nil if the timeout expires before a datagram arrives.
      def recv(timeout = nil)
        connect
        @socket.timeout = timeout unless timeout.nil?
        @socket.recv(MAX_PACKET_SIZE)
      rescue IO::WaitReadable
        nil
      end

      # Close the underlying socket.
      def close
        unless @socket.nil? || (@socket.respond_to?(:closed?) && @socket.closed?)
          @socket.close
        end
        @socket = nil
        @connected = false
      end

      def closed?
        @socket.nil? || (@socket.respond_to?(:closed?) && @socket.closed?)
      end
    end
  end
end

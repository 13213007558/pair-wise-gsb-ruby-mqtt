#!/usr/bin/env ruby

require 'logger'
require 'socket'
require 'thread'
require 'timeout'

require 'mqtt/version'

module MQTT
  # Default port number for unencrypted connections
  DEFAULT_PORT = 1883

  # Default port number for TLS/SSL encrypted connections
  DEFAULT_SSL_PORT = 8883

  # Super-class for other MQTT related exceptions
  class Exception < ::Exception
  end

  # A ProtocolException will be raised if there is a
  # problem with data received from a remote host
  class ProtocolException < MQTT::Exception
  end

  # A NotConnectedException will be raised when trying to
  # perform a function but no connection has been
  # established
  class NotConnectedException < MQTT::Exception
  end

  autoload :Client,   'mqtt/client'
  autoload :Packet,   'mqtt/packet'
  autoload :Proxy,    'mqtt/proxy'

  # MQTT-SN
  module SN
    # Default port number for unencrypted connections
    DEFAULT_PORT = 1883

    # A ProtocolException will be raised if there is a
    # problem with data received from a remote host
    class ProtocolException < MQTT::Exception
    end

    # A NotConnectedException will be raised when trying to perform
    # an operation that requires an established session
    class NotConnectedException < MQTT::NotConnectedException
    end

    # A TimeoutException will be raised when an acknowledgement or
    # response packet is not received in time
    class TimeoutException < MQTT::Exception
    end

    # A SessionLostException will be raised when the gateway sends an
    # unsolicited DISCONNECT, such as when the sleep duration expires
    class SessionLostException < MQTT::Exception
    end

    autoload :Client,       'mqtt/sn/client'
    autoload :Packet,       'mqtt/sn/packet'
    autoload :UDPTransport, 'mqtt/sn/udp_transport'
  end
end

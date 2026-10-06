# encoding: BINARY

$:.unshift(File.dirname(__FILE__))

require 'spec_helper'

# A controllable monotonic clock for the tests. When +tick_in_transport+
# is enabled, each empty poll of the transport advances time by one second,
# exercising timeout/keep-alive code paths without real waits.
class FakeClock
  attr_reader :now
  attr_accessor :tick_in_transport

  def initialize(start = 0)
    @now = start
    @tick_in_transport = false
  end

  def advance(seconds)
    @now += seconds
  end

  def auto_advance
    advance(1) if @tick_in_transport
  end

  def reset
    @now = 0
  end
end

class FakeUDPSocket
  class WouldWait < RuntimeError
    include IO::WaitReadable
  end

  attr_reader :sent, :connected_to, :timeout, :close_count

  def initialize
    @sent = []
    @connected_to = nil
    @timeout = nil
    @close_count = 0
  end

  def connect(host, port)
    @connected_to = [host, port]
  end

  def send(data, _flags)
    @sent << data
  end

  def recv(_length)
    raise WouldWait if @raise_wait

    @next_data
  end

  def timeout=(value)
    @timeout = value
  end

  def next_data=(data)
    @next_data = data
  end

  def simulate_timeout!
    @raise_wait = true
  end

  def close
    @close_count += 1
  end

  def closed?
    @close_count > 0
  end
end

describe MQTT::SN::UDPTransport do
  it 'connects lazily and sends datagrams to the fixed gateway' do
    socket = FakeUDPSocket.new
    transport = MQTT::SN::UDPTransport.new('gw.example', 1883, :socket => socket)
    transport.send('hello')
    expect(socket.connected_to).to eq(['gw.example', 1883])
    expect(socket.sent).to eq(['hello'])
  end

  it 'returns one datagram from recv' do
    socket = FakeUDPSocket.new
    socket.next_data = 'datagram'
    transport = MQTT::SN::UDPTransport.new('gw.example', 1883, :socket => socket)
    expect(transport.recv(2)).to eq('datagram')
    expect(socket.timeout).to eq(2)
  end

  it 'returns nil on a read timeout' do
    socket = FakeUDPSocket.new
    socket.simulate_timeout!
    transport = MQTT::SN::UDPTransport.new('gw.example', 1883, :socket => socket)
    expect(transport.recv(1)).to be_nil
  end

  it 'closes the socket exactly once' do
    socket = FakeUDPSocket.new
    transport = MQTT::SN::UDPTransport.new('gw.example', 1883, :socket => socket)
    transport.close
    transport.close
    expect(socket.close_count).to eq(1)
    expect(transport.closed?).to be(true)
  end
end

# Scripted datagram transport: records sent packets and serves queued
# datagrams from the fake gateway. Timing is interpreted against the
# injected fake clock.
class FakeTransport
  attr_reader :sent, :closed_count

  def initialize(clock)
    @clock = clock
    @inbox = Queue.new
    @sent = []
    @mutex = Mutex.new
    @closed = false
    @closed_count = 0
  end

  def send(data)
    @mutex.synchronize do
      raise IOError, 'transport closed' if @closed

      @sent << data
    end
  end

  def recv(timeout = nil)
    started = @clock.now
    real_started = Time.now
    loop do
      return @inbox.pop(true) unless @inbox.empty?

      # Advance the simulated clock while waiting, so time-based behaviour
      # (timeouts, keep-alive) triggers without real waiting.
      @clock.auto_advance
      sleep(0.005)
      elapsed = [@clock.now - started, Time.now - real_started].max
      if !timeout.nil? && elapsed >= timeout
        return nil
      end
    end
  end

  def deliver(packet)
    @inbox << (packet.is_a?(String) ? packet : packet.to_s)
  end

  def close
    @mutex.synchronize do
      unless @closed
        @closed = true
        @closed_count += 1
      end
    end
  end

  def closed?
    @mutex.synchronize { @closed }
  end

  def packets
    @sent.map { |data| MQTT::SN::Packet.parse(data) }
  end
end

def make_client(transport = nil, clock = nil, options = {})
  clock ||= FakeClock.new
  transport ||= FakeTransport.new(clock)
  client = MQTT::SN::Client.new(
    transport,
    {
      :client_id => 'sensor-1',
      :keep_alive => 10,
      :ack_timeout => 100,
      :clock => clock
    }.merge(options)
  )
  [client, transport, clock]
end

def connect_client(*args)
  client, transport, clock = make_client(*args)
  transport.deliver(MQTT::SN::Packet::Connack.new(:return_code => 0))
  client.connect
  [client, transport, clock]
end

def wait_for(deadline = Time.now + 2)
  sleep(0.005) until yield || Time.now > deadline
  raise 'wait_for timed out' unless yield
end

describe MQTT::SN::Client do
  def deliver_in_background(transport, packet)
    Thread.new do
      sleep(0.03)
      transport.deliver(packet)
    end
  end

  describe '#connect' do
    it 'sends a CONNECT carrying the client id and keep alive' do
      client, transport, = make_client
      transport.deliver(MQTT::SN::Packet::Connack.new(:return_code => 0))
      client.connect

      connect = transport.packets.first
      expect(connect).to be_a(MQTT::SN::Packet::Connect)
      expect(connect.client_id).to eq('sensor-1')
      expect(connect.keep_alive).to eq(10)
      expect(client).to be_connected
    end

    it 'raises on a rejected CONNACK and closes the transport' do
      client, transport, = make_client
      transport.deliver(MQTT::SN::Packet::Connack.new(:return_code => 1))
      expect { client.connect }.to raise_error(
        MQTT::SN::ProtocolException, /congestion/
      )
      expect(client).not_to be_connected
      expect(transport).to be_closed
    end

    it 'raises a timeout exception when no CONNACK arrives' do
      client, _transport, clock = make_client(nil, nil, :ack_timeout => 5)
      clock.tick_in_transport = true
      expect { client.connect }.to raise_error(
        MQTT::SN::TimeoutException, /connack/
      )
      expect(client).not_to be_connected
    end

    it 'disconnects when a block is given' do
      client, transport, = make_client
      transport.deliver(MQTT::SN::Packet::Connack.new(:return_code => 0))
      client.connect do |session|
        expect(session).to be_active
      end
      expect(client).not_to be_connected
      expect(transport).to be_closed
    end
  end

  describe '#publish' do
    it 'sends a QoS 0 PUBLISH without waiting' do
      client, transport, = connect_client
      result = client.publish('TT', 'hello')

      publish = transport.packets.grep(MQTT::SN::Packet::Publish).first
      expect(publish.qos).to eq(0)
      expect(publish.topic_id).to eq('TT')
      expect(publish.topic_id_type).to eq(:short)
      expect(publish.data).to eq('hello')
      expect(result).to be_nil
    end

    it 'sends a numeric short-topic PUBLISH for an integer topic id' do
      client, transport, = connect_client
      client.publish(42, 'hello')

      publish = transport.packets.grep(MQTT::SN::Packet::Publish).first
      expect(publish.topic_id).to eq(42)
      expect(publish.topic_id_type).to eq(:normal)
    end

    it 'waits for the matching PUBACK for QoS 1' do
      client, transport, clock = connect_client
      thread = Thread.new do
        packet = nil
        wait_for { transport.packets.grep(MQTT::SN::Packet::Publish).any? }
        publish = transport.packets.grep(MQTT::SN::Packet::Publish).first
        transport.deliver(MQTT::SN::Packet::Puback.new(
                            :topic_id => publish.topic_id,
                            :topic_id_type => publish.topic_id_type,
                            :id => publish.id,
                            :return_code => 0
                          ))
      end
      result = client.publish('TT', 'important', false, 1)
      thread.join
      expect(result).to be_a(MQTT::SN::Packet::Puback)
    end

    it 'ignores a PUBACK for a different packet id and waits for the right one' do
      client, transport, clock = connect_client
      Thread.new do
        wait_for { transport.packets.grep(MQTT::SN::Packet::Publish).any? }
        transport.deliver(MQTT::SN::Packet::Puback.new(
                            :topic_id => 1, :id => 999, :return_code => 0
                          ))
        publish = transport.packets.grep(MQTT::SN::Packet::Publish).first
        transport.deliver(MQTT::SN::Packet::Puback.new(
                            :topic_id => 1, :id => publish.id, :return_code => 0
                          ))
      end
      expect { client.publish('TT', 'x', false, 1) }.not_to raise_error
    end

    it 'raises on a non-zero PUBACK return code' do
      client, transport, = connect_client
      Thread.new do
        wait_for { transport.packets.grep(MQTT::SN::Packet::Publish).any? }
        publish = transport.packets.grep(MQTT::SN::Packet::Publish).first
        transport.deliver(MQTT::SN::Packet::Puback.new(
                            :topic_id => 1, :id => publish.id, :return_code => 1
                          ))
      end
      expect { client.publish('TT', 'x', false, 1) }.to raise_error(
        MQTT::SN::ProtocolException, /return code 1/
      )
    end

    it 'raises for unsupported QoS levels' do
      client, _transport, _clock = connect_client
      expect { client.publish('TT', 'x', false, 2) }.to raise_error(ArgumentError)
    end

    it 'refuses to publish while asleep' do
      client, transport, = connect_client
      transport.deliver(MQTT::SN::Packet::Disconnect.new(:duration => 300))
      client.sleep(300)
      expect { client.publish('TT', 'x') }.to raise_error(
        MQTT::SN::NotConnectedException
      )
    end
  end

  describe 'incoming PUBLISH' do
    it 'queues a QoS 0 PUBLISH and returns it from #get' do
      client, transport, = connect_client
      deliver_in_background(
        transport,
        MQTT::SN::Packet::Publish.new(
          :topic_id => 'TT', :topic_id_type => :short, :qos => 0, :data => 'hi'
        )
      )
      topic, data = client.get
      expect(topic).to eq('TT')
      expect(data).to eq('hi')
    end

    it 'acknowledges an inbound QoS 1 PUBLISH with a PUBACK' do
      client, transport, = connect_client
      deliver_in_background(
        transport,
        MQTT::SN::Packet::Publish.new(
          :topic_id => 'TT', :topic_id_type => :short, :qos => 1,
          :id => 7, :data => 'important'
        )
      )
      packet = client.get_packet
      expect(packet.data).to eq('important')
      wait_for { transport.packets.grep(MQTT::SN::Packet::Puback).any? }
      ack = transport.packets.grep(MQTT::SN::Packet::Puback).first
      expect(ack.id).to eq(7)
      # PUBACK carries no flags byte, so the topic id is numeric on the wire
      expect(ack.topic_id).to eq(0x5454)
    end

    it 'returns nil when the get timeout expires with no message' do
      client, _transport, clock = connect_client
      clock.tick_in_transport = true
      expect(client.get(1)).to be_nil
    end
  end

  describe 'on-line keep-alive' do
    it 'sends a client-id-less PINGREQ when the keep-alive interval passes' do
      client, transport, clock = connect_client
      clock.tick_in_transport = true
      responder = Thread.new do
        wait_for { transport.packets.grep(MQTT::SN::Packet::Pingreq).any? }
        transport.deliver(MQTT::SN::Packet::Pingresp.new)
        transport.deliver(MQTT::SN::Packet::Publish.new(
                            :topic_id => 'TT', :topic_id_type => :short,
                            :qos => 0, :data => 'after-heartbeat'
                          ))
      end
      packet = client.get_packet
      responder.join

      ping = transport.packets.grep(MQTT::SN::Packet::Pingreq).first
      expect(ping).not_to be_nil
      expect(ping.client_id).to be_nil
      expect(packet.data).to eq('after-heartbeat')
      expect(client).to be_active
    end

    it 'raises a protocol exception when the PINGRESP never arrives' do
      client, _transport, clock = connect_client
      clock.tick_in_transport = true
      expect { client.get_packet }.to raise_error(
        MQTT::SN::ProtocolException, /No PINGRESP/
      )
      expect(client).not_to be_connected
    end

    it 'renews the deadline each time a PINGRESP arrives' do
      client, transport, clock = connect_client
      clock.tick_in_transport = true
      responder = Thread.new do
        3.times do
          wait_for do
            sent = transport.packets.grep(MQTT::SN::Packet::Pingreq)
            sent.length > @responded.to_i
          end
          @responded = (@responded || 0) + 1
          transport.deliver(MQTT::SN::Packet::Pingresp.new)
        end
        transport.deliver(MQTT::SN::Packet::Publish.new(
                            :topic_id => 'TT', :topic_id_type => :short,
                            :qos => 0, :data => 'after-pings'
                          ))
      end
      packet = client.get_packet
      responder.join
      expect(packet.data).to eq('after-pings')
      expect(transport.packets.grep(MQTT::SN::Packet::Pingreq).length).to eq(3)
      expect(client).to be_active
    end
  end

  describe '#sleep' do
    it 'sends a DISCONNECT with the duration and stores the granted sleep' do
      client, transport, clock = connect_client
      transport.deliver(MQTT::SN::Packet::Disconnect.new(:duration => 250))
      granted = client.sleep(300)

      disconnect = transport.packets.grep(MQTT::SN::Packet::Disconnect).first
      expect(disconnect.duration).to eq(300)
      expect(granted).to eq(250)
      expect(client).to be_asleep
      expect(client.sleep_remaining).to eq(250)
    end

    it 'requires a positive integer duration' do
      client, _transport, _clock = connect_client
      expect { client.sleep(0) }.to raise_error(ArgumentError)
      expect(client).to be_active
    end

    it 'does not pump keep-alive while asleep' do
      client, transport, clock = connect_client
      transport.deliver(MQTT::SN::Packet::Disconnect.new(:duration => 300))
      client.sleep(300)
      clock.tick_in_transport = true
      sent = transport.packets.length
      client.get_packet(0.1)
      expect(transport.packets.length).to eq(sent)
      expect(transport.packets.grep(MQTT::SN::Packet::Pingreq)).to be_empty
    end
  end

  describe '#wake' do
    def sleep_client(client, transport)
      transport.deliver(MQTT::SN::Packet::Disconnect.new(:duration => 300))
      client.sleep(300)
    end

    it 'sends a PINGREQ with the client id and drains buffered messages until PINGRESP' do
      client, transport, = connect_client
      sleep_client(client, transport)
      transport.deliver(MQTT::SN::Packet::Publish.new(
                          :topic_id => 'T1', :topic_id_type => :short,
                          :qos => 0, :data => 'one'
                        ))
      transport.deliver(MQTT::SN::Packet::Publish.new(
                          :topic_id => 'T2', :topic_id_type => :short,
                          :qos => 1, :id => 9, :data => 'two'
                        ))
      transport.deliver(MQTT::SN::Packet::Pingresp.new)

      drained = client.wake
      expect(drained.map(&:data)).to eq(%w[one two])
      expect(client).to be_active

      pingreq = transport.packets.grep(MQTT::SN::Packet::Pingreq).last
      expect(pingreq.client_id).to eq('sensor-1')

      # The QoS 1 buffered message is acknowledged during the drain.
      ack = transport.packets.grep(MQTT::SN::Packet::Puback).first
      expect(ack.id).to eq(9)
    end

    it 'makes buffered messages available through #get after the wake-up' do
      client, transport, = connect_client
      sleep_client(client, transport)
      transport.deliver(MQTT::SN::Packet::Publish.new(
                          :topic_id => 'T1', :topic_id_type => :short,
                          :qos => 0, :data => 'backlog'
                        ))
      transport.deliver(MQTT::SN::Packet::Pingresp.new)
      client.wake

      topic, data = client.get
      expect([topic, data]).to eq(['T1', 'backlog'])
    end

    it 'yields each buffered message when a block is given' do
      client, transport, = connect_client
      sleep_client(client, transport)
      transport.deliver(MQTT::SN::Packet::Publish.new(
                          :topic_id => 'T1', :topic_id_type => :short,
                          :qos => 0, :data => 'one'
                        ))
      transport.deliver(MQTT::SN::Packet::Pingresp.new)

      seen = []
      client.wake { |packet| seen << packet.data }
      expect(seen).to eq(['one'])
    end

    it 'raises SessionLostException when the gateway answers DISCONNECT' do
      client, transport, = connect_client
      sleep_client(client, transport)
      transport.deliver(MQTT::SN::Packet::Disconnect.new)
      expect { client.wake }.to raise_error(MQTT::SN::SessionLostException)
      expect(client).not_to be_connected
      expect(transport).to be_closed
    end

    it 'returns to sleep when no wake-up reply arrives' do
      client, transport, clock = make_client(nil, nil, :ack_timeout => 5)
      transport.deliver(MQTT::SN::Packet::Connack.new(:return_code => 0))
      client.connect
      transport.deliver(MQTT::SN::Packet::Disconnect.new(:duration => 300))
      client.sleep(300)
      clock.tick_in_transport = true
      expect { client.wake }.to raise_error(MQTT::SN::TimeoutException)
      expect(client).to be_asleep
    end
  end

  describe 'stray and late replies' do
    it 'ignores a PINGRESP while waiting for a PUBACK' do
      client, transport, = connect_client
      Thread.new do
        wait_for { transport.packets.grep(MQTT::SN::Packet::Publish).any? }
        transport.deliver(MQTT::SN::Packet::Pingresp.new)
        publish = transport.packets.grep(MQTT::SN::Packet::Publish).first
        transport.deliver(MQTT::SN::Packet::Puback.new(
                            :topic_id => 1, :id => publish.id, :return_code => 0
                          ))
      end
      expect { client.publish('TT', 'x', false, 1) }.not_to raise_error
      expect(client).to be_active
    end

    it 'ignores a late CONNACK after connecting' do
      client, _transport, clock = connect_client
      clock.tick_in_transport = true
      expect(client.get_packet(0.1)).to be_nil
      expect(client).to be_active
    end

    it 'ignores a stray PUBACK during a wake-up drain and ends on PINGRESP' do
      client, transport, = connect_client
      transport.deliver(MQTT::SN::Packet::Disconnect.new(:duration => 300))
      client.sleep(300)
      transport.deliver(MQTT::SN::Packet::Puback.new(
                          :topic_id => 1, :id => 555, :return_code => 0
                        ))
      transport.deliver(MQTT::SN::Packet::Pingresp.new)
      drained = client.wake
      expect(drained).to be_empty
      expect(client).to be_active
    end
  end

  describe '#disconnect' do
    it 'sends a duration-less DISCONNECT when active and closes the transport' do
      client, transport, = connect_client
      client.disconnect
      disconnect = transport.packets.grep(MQTT::SN::Packet::Disconnect).first
      expect(disconnect.duration).to be_nil
      expect(transport).to be_closed
      expect(client).not_to be_connected
    end

    it 'sends no packet when disconnecting while asleep' do
      client, transport, = connect_client
      transport.deliver(MQTT::SN::Packet::Disconnect.new(:duration => 300))
      client.sleep(300)
      sent_count = transport.packets.length
      client.disconnect
      expect(transport.packets.length).to eq(sent_count)
      expect(client).not_to be_connected
    end

    it 'raises SessionLostException on an unsolicited DISCONNECT while active' do
      client, transport, = connect_client
      deliver_in_background(transport, MQTT::SN::Packet::Disconnect.new)
      expect { client.get_packet }.to raise_error(MQTT::SN::SessionLostException)
      expect(client).not_to be_connected
      expect(transport).to be_closed
    end
  end
end

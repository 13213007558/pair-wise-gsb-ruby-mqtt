# encoding: BINARY

$:.unshift(File.dirname(__FILE__))

require 'spec_helper'

# Monotonic clock driven by the test
class SnFakeClock
  attr_accessor :now

  def initialize
    @now = 0.0
  end

  def sleep(seconds)
    @now += seconds
  end
end

# In-memory datagram transport; a responder block can be installed to
# play the role of the gateway
class SnFakeTransport
  attr_reader :sent
  attr_accessor :responder

  def initialize
    @sent = []
    @inbox = []
  end

  def send(bytes)
    @sent << bytes
    @responder.call(bytes) if @responder
  end

  def recv
    @inbox.shift
  end

  def inject(packet)
    @inbox << packet.to_s
  end

  def packets
    @sent.map { |bytes| MQTT::SN::Packet.parse(bytes) }
  end
end


describe MQTT::SN::Session do
  let(:clock) { SnFakeClock.new }
  let(:transport) { SnFakeTransport.new }
  let(:events) { [] }
  let(:messages) { [] }
  let(:session) do
    s = MQTT::SN::Session.new(
      :transport => transport,
      :clock => clock,
      :client_id => 'sensor-1',
      :keep_alive => 30,
      :ack_timeout => 5,
      :max_retries => 2
    )
    s.on_event = lambda { |event, *details| events << [event, *details] }
    s.on_message = lambda { |packet| messages << packet }
    s
  end

  def answer_connect
    transport.responder = lambda do |bytes|
      packet = MQTT::SN::Packet.parse(bytes)
      if packet.is_a?(MQTT::SN::Packet::Connect)
        transport.inject(MQTT::SN::Packet::Connack.new(:return_code => 0x00))
      end
    end
  end

  def connect_session
    answer_connect
    session.connect
    transport.sent.clear
  end

  describe 'when connecting' do
    it 'should become awake once the gateway accepts' do
      answer_connect
      expect(session.connect).to be_truthy
      expect(session.state).to eq(:awake)
      expect(session).to be_awake
      expect(events).to include([:connected])
    end

    it 'should send a CONNECT packet with the client id and keep-alive' do
      answer_connect
      session.connect
      packet = transport.packets.last
      expect(packet).to be_a(MQTT::SN::Packet::Connect)
      expect(packet.client_id).to eq('sensor-1')
      expect(packet.keep_alive).to eq(30)
    end

    it 'should raise when the gateway rejects the connection' do
      transport.responder = lambda do |_bytes|
        transport.inject(MQTT::SN::Packet::Connack.new(:return_code => 0x01))
      end
      expect { session.connect }.to raise_error(
        MQTT::SN::ConnectException, 'Rejected: congestion'
      )
      expect(session.state).to eq(:offline)
    end

    it 'should retransmit and then raise when the gateway is silent' do
      expect { session.connect }.to raise_error(MQTT::SN::TimeoutException)
      expect(transport.packets.count { |p| p.is_a?(MQTT::SN::Packet::Connect) }).to eq(3)
      expect(events.select { |e| e.first == :timeout }.length).to eq(3)
      expect(session.state).to eq(:offline)
    end

    it 'should ignore a duplicate CONNACK that arrives late' do
      transport.responder = lambda do |_bytes|
        transport.inject(MQTT::SN::Packet::Connack.new(:return_code => 0x00))
        transport.inject(MQTT::SN::Packet::Connack.new(:return_code => 0x00))
      end
      session.connect
      expect(session.poll).to eq(1)
      expect(session.state).to eq(:awake)
    end
  end

  describe 'when publishing' do
    before { connect_session }

    it 'should send QoS 0 messages without waiting for an ack' do
      id = session.publish(0x10, 'hello', :qos => 0)
      expect(id).to eq(0)
      packet = transport.packets.last
      expect(packet).to be_a(MQTT::SN::Packet::Publish)
      expect(packet.topic_id).to eq(0x10)
      expect(packet.data).to eq('hello')
      expect(packet.qos).to eq(0)
    end

    it 'should wait for the PUBACK of QoS 1 messages' do
      transport.responder = lambda do |bytes|
        packet = MQTT::SN::Packet.parse(bytes)
        if packet.is_a?(MQTT::SN::Packet::Publish)
          transport.inject(
            MQTT::SN::Packet::Puback.new(:topic_id => packet.topic_id, :id => packet.id)
          )
        end
      end
      id = session.publish(0x10, 'hello', :qos => 1)
      expect(id).to be > 0
      expect(transport.packets.last.id).to eq(id)
    end

    it 'should retransmit QoS 1 messages and raise if never acked' do
      expect { session.publish(0x10, 'hello', :qos => 1) }.to raise_error(
        MQTT::SN::TimeoutException
      )
      publishes = transport.packets.select { |p| p.is_a?(MQTT::SN::Packet::Publish) }
      expect(publishes.length).to eq(3)
      expect(publishes.map(&:id).uniq.length).to eq(1)
    end

    it 'should ignore a late duplicate PUBACK' do
      transport.responder = lambda do |bytes|
        packet = MQTT::SN::Packet.parse(bytes)
        if packet.is_a?(MQTT::SN::Packet::Publish)
          puback = MQTT::SN::Packet::Puback.new(:topic_id => packet.topic_id, :id => packet.id)
          transport.inject(puback)
          transport.inject(puback)
        end
      end
      session.publish(0x10, 'hello', :qos => 1)
      expect(session.poll).to eq(1)
      expect(session.state).to eq(:awake)
    end

    it 'should raise when the session is not awake' do
      expect { MQTT::SN::Session.new(:transport => transport, :clock => clock, :client_id => 'x').publish(0x10, 'x') }.to raise_error(MQTT::NotConnectedException)
    end
  end

  describe 'when subscribing' do
    before { connect_session }

    it 'should return the topic id granted by the gateway' do
      transport.responder = lambda do |bytes|
        packet = MQTT::SN::Packet.parse(bytes)
        if packet.is_a?(MQTT::SN::Packet::Subscribe)
          transport.inject(
            MQTT::SN::Packet::Suback.new(:id => packet.id, :topic_id => 0x2A, :return_code => 0x00)
          )
        end
      end
      expect(session.subscribe('sensors/temp', :qos => 1)).to eq(0x2A)
    end
  end

  describe 'when receiving messages' do
    before { connect_session }

    it 'should deliver incoming publishes and ack QoS 1 ones' do
      transport.inject(
        MQTT::SN::Packet::Publish.new(:topic_id => 0x2A, :id => 7, :data => '21.5', :qos => 1)
      )
      session.poll
      expect(messages.length).to eq(1)
      expect(messages.first.data).to eq('21.5')
      puback = transport.packets.last
      expect(puback).to be_a(MQTT::SN::Packet::Puback)
      expect(puback.id).to eq(7)
      expect(events).to include([:message, messages.first])
    end

    it 'should deliver a duplicated QoS 1 publish only once but ack it twice' do
      2.times do
        transport.inject(
          MQTT::SN::Packet::Publish.new(:topic_id => 0x2A, :id => 7, :data => '21.5', :qos => 1)
        )
      end
      session.poll
      expect(messages.length).to eq(1)
      pubacks = transport.packets.select { |p| p.is_a?(MQTT::SN::Packet::Puback) }
      expect(pubacks.length).to eq(2)
    end
  end

  describe 'keep-alive while awake' do
    before { connect_session }

    it 'should not ping before the keep-alive interval elapses' do
      session.tick
      expect(transport.sent).to be_empty
    end

    it 'should send a PINGREQ without a client id once the interval elapses' do
      transport.responder = lambda do |_bytes|
        transport.inject(MQTT::SN::Packet::Pingresp.new)
      end
      clock.sleep(31)
      session.tick
      ping = transport.packets.last
      expect(ping).to be_a(MQTT::SN::Packet::Pingreq)
      expect(ping.client_id).to be_nil
      expect(session.state).to eq(:awake)
    end

    it 'should declare the session lost when heartbeats go unanswered' do
      clock.sleep(31)
      session.tick
      expect(session.state).to eq(:lost)
      expect(events).to include([:session_lost, :keepalive_timeout])
    end
  end

  describe 'when sleeping' do
    before { connect_session }

    def answer_sleep
      transport.responder = lambda do |bytes|
        packet = MQTT::SN::Packet.parse(bytes)
        if packet.is_a?(MQTT::SN::Packet::Disconnect)
          transport.inject(MQTT::SN::Packet::Disconnect.new)
        end
      end
    end

    it 'should send DISCONNECT with the duration and become asleep' do
      answer_sleep
      session.sleep(600)
      packet = transport.packets.select { |p| p.is_a?(MQTT::SN::Packet::Disconnect) }.first
      expect(packet.duration).to eq(600)
      expect(session.state).to eq(:asleep)
      expect(session).to be_asleep
      expect(session.sleep_duration).to eq(600)
      expect(events).to include([:asleep, 600])
    end

    it 'should not send keep-alives while asleep' do
      answer_sleep
      session.sleep(600)
      transport.sent.clear
      clock.sleep(3600)
      session.tick
      expect(transport.sent).to be_empty
      expect(session.state).to eq(:asleep)
    end

    it 'should refuse to publish while asleep' do
      answer_sleep
      session.sleep(600)
      expect { session.publish(0x10, 'x') }.to raise_error(/asleep/)
    end

    it 'should raise when waking a session that is not asleep' do
      expect { session.wake }.to raise_error(/not asleep/)
    end
  end

  describe 'when waking up' do
    before { connect_session }

    def answer_sleep
      transport.responder = lambda do |bytes|
        packet = MQTT::SN::Packet.parse(bytes)
        if packet.is_a?(MQTT::SN::Packet::Disconnect)
          transport.inject(MQTT::SN::Packet::Disconnect.new)
        end
      end
      session.sleep(600)
      transport.sent.clear
    end

    it 'should ping with the client id and drain buffered messages until PINGRESP' do
      answer_sleep
      transport.responder = lambda do |bytes|
        packet = MQTT::SN::Packet.parse(bytes)
        if packet.is_a?(MQTT::SN::Packet::Pingreq)
          transport.inject(MQTT::SN::Packet::Publish.new(:topic_id => 0x2A, :id => 1, :data => 'a', :qos => 1))
          transport.inject(MQTT::SN::Packet::Publish.new(:topic_id => 0x2A, :id => 2, :data => 'b', :qos => 0))
          transport.inject(MQTT::SN::Packet::Pingresp.new)
        end
      end
      session.wake
      ping = transport.packets.select { |p| p.is_a?(MQTT::SN::Packet::Pingreq) }.first
      expect(ping.client_id).to eq('sensor-1')
      expect(messages.map(&:data)).to eq(['a', 'b'])
      expect(session.state).to eq(:awake)
      expect(session.sleep_duration).to be_nil
      expect(events).to include([:awake])
    end

    it 'should ignore a duplicate PINGRESP that arrives after waking' do
      answer_sleep
      transport.responder = lambda do |bytes|
        packet = MQTT::SN::Packet.parse(bytes)
        if packet.is_a?(MQTT::SN::Packet::Pingreq)
          transport.inject(MQTT::SN::Packet::Pingresp.new)
          transport.inject(MQTT::SN::Packet::Pingresp.new)
        end
      end
      session.wake
      expect(session.poll).to eq(1)
      expect(session.state).to eq(:awake)
    end

    it 'should declare the session lost when the gateway never answers' do
      answer_sleep
      transport.responder = nil
      session.wake
      expect(session.state).to eq(:lost)
      expect(events).to include([:session_lost, :wake_timeout])
    end
  end

  describe 'when the gateway drops the session' do
    before { connect_session }

    it 'should become lost on an unsolicited DISCONNECT' do
      transport.inject(MQTT::SN::Packet::Disconnect.new)
      session.poll
      expect(session.state).to eq(:lost)
      expect(events).to include([:session_lost, :gateway_disconnect])
    end

    it 'should refuse further work once lost' do
      transport.inject(MQTT::SN::Packet::Disconnect.new)
      session.poll
      expect { session.publish(0x10, 'x') }.to raise_error(MQTT::NotConnectedException)
    end
  end

  describe 'when disconnecting' do
    before { connect_session }

    it 'should send DISCONNECT without a duration and close' do
      transport.responder = lambda do |bytes|
        packet = MQTT::SN::Packet.parse(bytes)
        if packet.is_a?(MQTT::SN::Packet::Disconnect)
          transport.inject(MQTT::SN::Packet::Disconnect.new)
        end
      end
      session.disconnect
      packet = transport.packets.select { |p| p.is_a?(MQTT::SN::Packet::Disconnect) }.first
      expect(packet.duration).to be_nil
      expect(session.state).to eq(:closed)
      expect(events).to include([:disconnected, false])
    end

    it 'should close even if the gateway never acknowledges' do
      session.disconnect
      expect(session.state).to eq(:closed)
      expect(events).to include([:disconnected, false])
    end

    it 'should be observable that the client disconnected from sleep' do
      transport.responder = lambda do |bytes|
        packet = MQTT::SN::Packet.parse(bytes)
        if packet.is_a?(MQTT::SN::Packet::Disconnect)
          transport.inject(MQTT::SN::Packet::Disconnect.new)
        end
      end
      session.sleep(600)
      session.disconnect
      expect(session.state).to eq(:closed)
      expect(events).to include([:disconnected, true])
    end

    it 'should refuse further work once closed' do
      session.disconnect
      expect { session.publish(0x10, 'x') }.to raise_error(MQTT::NotConnectedException)
    end
  end
end

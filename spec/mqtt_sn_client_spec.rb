# encoding: BINARY
# Encoding is set to binary, so that the binary packets aren't validated as UTF-8

$:.unshift(File.dirname(__FILE__))

require 'spec_helper'

# A fake UDP transport layer: delivers outbound datagrams to the
# currently addressed gateway (SEARCHGW is broadcast to all) and
# keeps the parsed packets for inspection
class FakeSnTransport
  attr_reader :sent
  attr_accessor :destination

  def initialize
    @sent = []
    @gateways = []
    @destination = nil
  end

  def add_gateway(gateway)
    @gateways << gateway
  end

  def send_data(data)
    packet = MQTT::SN::Packet.parse(data)
    @sent << packet
    if packet.is_a?(MQTT::SN::Packet::Searchgw)
      @gateways.each { |gateway| gateway.receive(packet) }
    elsif @destination
      @destination.receive(packet)
    end
  end
end

# A fake MQTT-SN gateway: answers SEARCHGW, CONNECT, REGISTER and
# QoS 1 PUBLISH packets, assigning topic IDs from its own counter
class FakeSnGateway
  attr_reader :gateway_id, :registrations, :published
  attr_accessor :client

  def initialize(gateway_id, first_topic_id)
    @gateway_id = gateway_id
    @next_topic_id = first_topic_id
    @registrations = {}
    @published = []
  end

  def receive(packet)
    response =
      case packet
      when MQTT::SN::Packet::Searchgw
        MQTT::SN::Packet::Gwinfo.new(:gateway_id => @gateway_id)
      when MQTT::SN::Packet::Connect
        MQTT::SN::Packet::Connack.new(:return_code => 0x00)
      when MQTT::SN::Packet::Register
        topic_id = @next_topic_id
        @next_topic_id += 1
        @registrations[packet.topic_name] = topic_id
        MQTT::SN::Packet::Regack.new(
          :id => packet.id,
          :topic_id => topic_id,
          :return_code => 0x00
        )
      when MQTT::SN::Packet::Publish
        @published << [packet.topic_id_type, packet.topic_id, packet.data, packet.qos]
        if packet.qos == 1
          MQTT::SN::Packet::Puback.new(
            :topic_id => packet.topic_id.is_a?(Integer) ? packet.topic_id : 0,
            :id => packet.id,
            :return_code => 0x00
          )
        end
      end
    @client.handle_packet(response) if response && @client
  end
end

describe MQTT::SN::Client do
  let(:transport) { FakeSnTransport.new }
  let(:gateway1) { FakeSnGateway.new(1, 100) }
  let(:gateway2) { FakeSnGateway.new(2, 200) }
  let(:client) { MQTT::SN::Client.new(transport, :client_id => 'spec-client') }

  before(:each) do
    transport.add_gateway(gateway1)
    transport.add_gateway(gateway2)
    transport.destination = gateway1
    gateway1.client = client
    gateway2.client = client
  end

  def connect_to_gateway1
    client.discover
    client.connect
  end

  describe "when created" do
    it "should start in the offline state" do
      expect(client.state).to eq(:offline)
      expect(client.status).to eq(:ok)
    end

    it "should require a transport responding to send_data" do
      expect { MQTT::SN::Client.new(Object.new) }.to raise_error(ArgumentError)
    end
  end

  describe "gateway discovery" do
    it "should broadcast a SEARCHGW packet" do
      client.discover
      expect(transport.sent.first).to be_a(MQTT::SN::Packet::Searchgw)
    end

    it "should discover both gateways and select the first one" do
      client.discover
      expect(client.gateways.keys).to contain_exactly(1, 2)
      expect(client.gateway_id).to eq(1)
      expect(client.state).to eq(:disconnected)
    end
  end

  describe "connecting" do
    it "should raise an exception when no gateway is selected" do
      expect { client.connect }.to raise_error(MQTT::Exception, /No gateway/)
    end

    it "should become connected after a CONNACK" do
      connect_to_gateway1
      expect(client.state).to eq(:connected)
      expect(client).to be_connected
    end

    it "should refuse to publish while not connected" do
      expect { client.publish('a/b', 'x') }.to raise_error(MQTT::NotConnectedException)
    end
  end

  describe "registering and publishing normal topics" do
    before(:each) { connect_to_gateway1 }

    it "should register a topic name and store the gateway-assigned ID" do
      client.register('sensors/temp')
      expect(client.registered?('sensors/temp')).to be_truthy
      expect(client.topic_id_for('sensors/temp')).to eq(100)
    end

    it "should auto-register and queue a QoS 0 publish until the REGACK arrives" do
      client.publish('sensors/temp', '21')
      expect(gateway1.registrations).to eq('sensors/temp' => 100)
      expect(gateway1.published).to eq([[:normal, 100, '21', 0]])
    end

    it "should reuse the registration for subsequent publishes" do
      client.publish('sensors/temp', '21')
      client.publish('sensors/temp', '22')
      expect(gateway1.registrations.size).to eq(1)
      expect(gateway1.published).to eq([[:normal, 100, '21', 0], [:normal, 100, '22', 0]])
    end
  end

  describe "publishing with predefined and short topics" do
    before(:each) { connect_to_gateway1 }

    it "should publish to a predefined topic ID without registering" do
      client.publish(7, 'preset', :topic_type => :predefined)
      expect(gateway1.registrations).to be_empty
      expect(gateway1.published).to eq([[:predefined, 7, 'preset', 0]])
    end

    it "should publish to a short topic name without registering" do
      client.publish('ab', 'short', :topic_type => :short)
      expect(gateway1.registrations).to be_empty
      expect(gateway1.published).to eq([[:short, 'ab', 'short', 0]])
    end

    it "should reject a predefined topic that is not an Integer" do
      expect {
        client.publish('nope', 'x', :topic_type => :predefined)
      }.to raise_error(ArgumentError, /predefined/)
    end

    it "should reject a short topic name that is not 2 characters" do
      expect {
        client.publish('toolong', 'x', :topic_type => :short)
      }.to raise_error(ArgumentError, /short/)
    end
  end

  describe "publishing at QoS 1" do
    before(:each) { connect_to_gateway1 }

    it "should complete the publish when the PUBACK arrives" do
      client.register('sensors/temp')
      message_id = client.publish('sensors/temp', '21', :qos => 1)
      expect(client.pending_pubacks).to be_empty
      expect(gateway1.published).to eq([[:normal, 100, '21', 1]])
      expect(message_id).to be > 0
    end

    it "should keep the publish pending when no PUBACK is received" do
      client.register('sensors/temp')
      gateway1.client = nil
      gateway2.client = nil
      message_id = client.publish('sensors/temp', '21', :qos => 1)
      expect(client.pending_pubacks.keys).to eq([message_id])
    end
  end

  describe "switching gateways" do
    it "should discard registrations and re-register on the new gateway" do
      connect_to_gateway1
      client.publish('sensors/temp', '21')
      expect(client.topic_id_for('sensors/temp')).to eq(100)

      client.use_gateway(2)
      expect(client.state).to eq(:disconnected)
      expect(client.registered?('sensors/temp')).to be_falsey

      transport.destination = gateway2
      client.connect
      client.publish('sensors/temp', '22')
      expect(client.topic_id_for('sensors/temp')).to eq(200)
      expect(gateway2.published).to eq([[:normal, 200, '22', 0]])
      expect(gateway1.published).to eq([[:normal, 100, '21', 0]])
    end

    it "should keep the mapping when the same gateway is selected again" do
      connect_to_gateway1
      client.register('sensors/temp')
      client.use_gateway(1)
      expect(client.topic_id_for('sensors/temp')).to eq(100)
    end
  end

  describe "unsupported features" do
    before(:each) { connect_to_gateway1 }

    it "should report an unsupported status when a QoS -1 publish is received" do
      client.handle_packet(
        MQTT::SN::Packet::Publish.new(
          :qos => -1, :topic_id_type => :short, :topic_id => 'ab', :data => 'x'
        )
      )
      expect(client.status).to eq(:unsupported)
      expect(client.status_message).to match(/QoS -1/)
    end

    it "should report an unsupported status when asked to publish at QoS -1" do
      client.publish('sensors/temp', '21', :qos => -1)
      expect(client.status).to eq(:unsupported)
      expect(client.status_message).to match(/QoS -1/)
      expect(gateway1.published).to be_empty
    end

    it "should report an unsupported status when a sleep disconnect is received" do
      client.handle_packet(MQTT::SN::Packet::Disconnect.new(:duration => 60))
      expect(client.status).to eq(:unsupported)
      expect(client.status_message).to match(/[Ss]leep/)
      expect(client.state).to eq(:connected)
    end

    it "should clear the unsupported status on the next command" do
      client.handle_packet(MQTT::SN::Packet::Disconnect.new(:duration => 60))
      client.register('sensors/temp')
      expect(client.status).to eq(:ok)
    end
  end

  describe "receiving publishes from the gateway" do
    before(:each) { connect_to_gateway1 }

    it "should acknowledge a QoS 1 publish with a PUBACK" do
      client.handle_packet(
        MQTT::SN::Packet::Publish.new(:qos => 1, :topic_id => 100, :id => 9, :data => 'x')
      )
      puback = transport.sent.last
      expect(puback).to be_a(MQTT::SN::Packet::Puback)
      expect(puback.id).to eq(9)
    end
  end

  describe "disconnecting" do
    it "should go offline and drop registrations with a clean session" do
      connect_to_gateway1
      client.register('sensors/temp')
      client.disconnect
      expect(client.state).to eq(:offline)
      expect(client.registered?('sensors/temp')).to be_falsey
    end
  end
end

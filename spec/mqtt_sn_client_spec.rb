# encoding: BINARY
# Encoding is set to binary, so that the binary packets aren't validated as UTF-8

$:.unshift(File.dirname(__FILE__))

require 'spec_helper'

# A fake MQTT-SN gateway used to test the client state machine.
# It responds synchronously to packets routed to it by the
# FakeSnTransport below.
class FakeSnGateway
  attr_reader :id, :address, :received
  attr_accessor :connack_return_code
  attr_accessor :respond_to_publish

  def initialize(id, address, first_topic_id)
    @id = id
    @address = address
    @next_topic_id = first_topic_id
    @received = []
    @connack_return_code = 0x00
    @respond_to_publish = true
    @registrations = {}
  end

  # Called by the transport with a raw datagram from the client
  def receive(data, client)
    packet = MQTT::SN::Packet.parse(data)
    @received << packet
    case packet
    when MQTT::SN::Packet::Searchgw
      reply(client, MQTT::SN::Packet::Gwinfo.new(:gateway_id => id))
    when MQTT::SN::Packet::Connect
      reply(client, MQTT::SN::Packet::Connack.new(:return_code => connack_return_code))
    when MQTT::SN::Packet::Register
      topic_id = @registrations[packet.topic_name] ||= allocate_topic_id
      reply(client, MQTT::SN::Packet::Regack.new(
        :id => packet.id,
        :topic_id => topic_id,
        :return_code => 0x00
      ))
    when MQTT::SN::Packet::Publish
      if packet.qos == 1 && respond_to_publish
        reply(client, MQTT::SN::Packet::Puback.new(
          :id => packet.id,
          :topic_id => packet.topic_id_type == :short ? 0 : packet.topic_id,
          :return_code => 0x00
        ))
      end
    end
  end

  # Send a gateway-initiated packet to the client
  def send_to(client, packet)
    client.receive_packet(packet, address)
  end

  def received_of_type(type)
    received.select { |packet| packet.is_a?(type) }
  end

  private

  def allocate_topic_id
    topic_id = @next_topic_id
    @next_topic_id += 1
    topic_id
  end

  def reply(client, packet)
    client.receive_packet(packet, address)
  end
end

# Caller-provided UDP transport double: routes outgoing datagrams
# to the fake gateways (broadcast when the address is nil)
class FakeSnTransport
  attr_reader :sent
  attr_accessor :client

  def initialize(gateways)
    @gateways = gateways
    @sent = []
  end

  def send(data, address)
    @sent << [data, address]
    targets = address.nil? ? @gateways : @gateways.select { |gw| gw.address == address }
    targets.each { |gw| gw.receive(data, @client) }
  end
end

describe MQTT::SN::Client do
  let(:gateway_a) { FakeSnGateway.new(1, '10.0.0.1:1883', 1) }
  let(:gateway_b) { FakeSnGateway.new(2, '10.0.0.2:1883', 101) }
  let(:client) do
    client = MQTT::SN::Client.new(
      :client_id => 'rspec-sn-client',
      :transport => transport,
      :logger => Logger.new(File::NULL)
    )
    transport.client = client
    client
  end
  let(:transport) { FakeSnTransport.new([gateway_a, gateway_b]) }

  describe 'gateway discovery' do
    it 'broadcasts a SEARCHGW and records both gateways from GWINFO replies' do
      client.search_gateway
      expect(client.gateways.length).to eq(2)
      expect(client.gateways.map(&:id)).to contain_exactly(1, 2)
      expect(client.gateways.map(&:address)).to contain_exactly('10.0.0.1:1883', '10.0.0.2:1883')
    end

    it 'broadcasts the SEARCHGW to all gateways' do
      client.search_gateway(2)
      expect(gateway_a.received_of_type(MQTT::SN::Packet::Searchgw).length).to eq(1)
      expect(gateway_b.received_of_type(MQTT::SN::Packet::Searchgw).length).to eq(1)
      expect(gateway_a.received.first.radius).to eq(2)
    end

    it 'records gateways announced via ADVERTISE' do
      status = client.receive_packet(
        MQTT::SN::Packet::Advertise.new(:gateway_id => 9, :duration => 60),
        '10.0.0.9:1883'
      )
      expect(status).to eq(:gateway_found)
      gateway = client.gateways.find { |gw| gw.id == 9 }
      expect(gateway.address).to eq('10.0.0.9:1883')
      expect(gateway.duration).to eq(60)
    end
  end

  describe 'connecting' do
    before { client.search_gateway }

    it 'connects to a discovered gateway' do
      client.connect(1)
      expect(client.state).to eq(:connected)
      expect(client.gateway.id).to eq(1)
      connect = gateway_a.received_of_type(MQTT::SN::Packet::Connect).first
      expect(connect.client_id).to eq('rspec-sn-client')
    end

    it 'returns :rejected and stays disconnected when the CONNACK is negative' do
      gateway_a.connack_return_code = 0x01
      client.connect(1)
      expect(client.state).to eq(:disconnected)
      expect(client.gateway).to be_nil
    end

    it 'raises for an unknown gateway id' do
      expect { client.connect(99) }.to raise_error(ArgumentError)
    end

    it 'raises a NotConnectedException when publishing while disconnected' do
      expect { client.publish('a/b', 'hello') }.to raise_error(MQTT::NotConnectedException)
    end
  end

  describe 'topic registration and publishing' do
    before do
      client.search_gateway
      client.connect(1)
    end

    it 'registers a normal topic name and stores the assigned topic id' do
      client.register_topic('a/b')
      expect(client.registered_topic_id('a/b')).to eq(1)
      register = gateway_a.received_of_type(MQTT::SN::Packet::Register).first
      expect(register.topic_name).to eq('a/b')
      expect(register.topic_id).to eq(0)
    end

    it 'auto-registers a normal topic name on first publish' do
      client.publish('a/b', 'hello')
      expect(client.registered_topic_id('a/b')).to eq(1)
      publish = gateway_a.received_of_type(MQTT::SN::Packet::Publish).first
      expect(publish.topic_id_type).to eq(:normal)
      expect(publish.topic_id).to eq(1)
      expect(publish.data).to eq('hello')
      expect(publish.qos).to eq(0)
    end

    it 'publishes with QoS 1 and completes on PUBACK' do
      gateway_a.respond_to_publish = false
      message_id = client.publish(7, 'hello', :topic_id_type => :predefined, :qos => 1)
      expect(message_id).to be > 0
      publish = gateway_a.received_of_type(MQTT::SN::Packet::Publish).first
      expect(publish.qos).to eq(1)
      expect(publish.id).to eq(message_id)

      status = client.receive_packet(
        MQTT::SN::Packet::Puback.new(:id => message_id, :topic_id => 7, :return_code => 0x00),
        gateway_a.address
      )
      expect(status).to eq(:published)
    end

    it 'returns :rejected for a negative PUBACK' do
      gateway_a.respond_to_publish = false
      message_id = client.publish(7, 'hello', :topic_id_type => :predefined, :qos => 1)
      status = client.receive_packet(
        MQTT::SN::Packet::Puback.new(:id => message_id, :topic_id => 7, :return_code => 0x02),
        gateway_a.address
      )
      expect(status).to eq(:rejected)
    end

    it 'publishes to a predefined topic id without registering' do
      message_id = client.publish(7, 'hello', :topic_id_type => :predefined, :qos => 1)
      expect(message_id).to be > 0
      expect(gateway_a.received_of_type(MQTT::SN::Packet::Register)).to be_empty
      publish = gateway_a.received_of_type(MQTT::SN::Packet::Publish).first
      expect(publish.topic_id_type).to eq(:predefined)
      expect(publish.topic_id).to eq(7)
    end

    it 'publishes to a short topic name without registering' do
      client.publish('ab', 'hello', :topic_id_type => :short)
      expect(gateway_a.received_of_type(MQTT::SN::Packet::Register)).to be_empty
      publish = gateway_a.received_of_type(MQTT::SN::Packet::Publish).first
      expect(publish.topic_id_type).to eq(:short)
      expect(publish.topic_id).to eq('ab')
    end

    it 'keeps the three topic representations distinct' do
      client.publish('a/b', 'normal')
      client.publish(7, 'predef', :topic_id_type => :predefined)
      client.publish('ab', 'short', :topic_id_type => :short)
      types = gateway_a.received_of_type(MQTT::SN::Packet::Publish).map(&:topic_id_type)
      expect(types).to eq([:normal, :predefined, :short])
    end

    it 'rejects QoS -1 publish requests' do
      expect {
        client.publish('a/b', 'hello', :qos => -1)
      }.to raise_error(ArgumentError, /not supported/)
    end

    it 'rejects short topics that are not two characters' do
      expect {
        client.publish('abc', 'hello', :topic_id_type => :short)
      }.to raise_error(ArgumentError)
    end
  end

  describe 'switching gateways' do
    before do
      client.search_gateway
      client.connect(1)
      client.publish('a/b', 'on gateway a')
    end

    it 'does not reuse registration ids from the previous gateway' do
      expect(client.registered_topic_id('a/b')).to eq(1)

      client.connect(2)
      expect(client.gateway.id).to eq(2)
      expect(client.registered_topic_id('a/b')).to be_nil

      client.publish('a/b', 'on gateway b')
      # Gateway B assigns its own topic ids (starting at 101)
      expect(client.registered_topic_id('a/b')).to eq(101)
      register = gateway_b.received_of_type(MQTT::SN::Packet::Register).first
      expect(register.topic_name).to eq('a/b')
      publish = gateway_b.received_of_type(MQTT::SN::Packet::Publish).first
      expect(publish.topic_id).to eq(101)
    end

    it 'keeps registrations when reconnecting to the same gateway' do
      client.connect(1)
      expect(client.registered_topic_id('a/b')).to eq(1)
    end
  end

  describe 'unsupported features' do
    before do
      client.search_gateway
      client.connect(1)
    end

    it 'reports :not_supported for an incoming QoS -1 PUBLISH' do
      packet = MQTT::SN::Packet::Publish.new(
        :qos => -1,
        :topic_id_type => :short,
        :topic_id => 'ab',
        :data => 'hello'
      )
      status = client.receive_packet(packet, gateway_a.address)
      expect(status).to eq(:not_supported)
    end

    it 'reports :not_supported for a sleep request (DISCONNECT with duration)' do
      status = client.receive_packet(
        MQTT::SN::Packet::Disconnect.new(:duration => 60),
        gateway_a.address
      )
      expect(status).to eq(:not_supported)
      expect(client.state).to eq(:connected)
    end

    it 'reports :not_supported for Will prompts' do
      expect(client.receive_packet(MQTT::SN::Packet::Willtopicreq.new, gateway_a.address)).to eq(:not_supported)
      expect(client.receive_packet(MQTT::SN::Packet::Willmsgreq.new, gateway_a.address)).to eq(:not_supported)
    end

    it 'disconnects when the gateway sends a plain DISCONNECT' do
      status = client.receive_packet(MQTT::SN::Packet::Disconnect.new, gateway_a.address)
      expect(status).to eq(:disconnected)
      expect(client.state).to eq(:disconnected)
    end
  end

  describe 'receiving raw datagrams' do
    it 'parses and handles a raw GWINFO datagram' do
      data = MQTT::SN::Packet::Gwinfo.new(:gateway_id => 5).to_s
      status = client.receive(data, '10.0.0.5:1883')
      expect(status).to eq(:gateway_found)
      expect(client.gateways.map(&:id)).to include(5)
    end
  end
end

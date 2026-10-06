# encoding: BINARY

$:.unshift(File.dirname(__FILE__))

require 'spec_helper'
require 'mqtt'

describe MQTT::Client, 'persistent sessions' do
  class SessionFakeSocket
    attr_reader :written

    def initialize(session_present)
      @session_present = session_present
      @written = String.new.force_encoding(Encoding::BINARY)
      @closed = false
    end

    def write(data)
      @written << data.to_s
      data.to_s.bytesize
    end

    def closed?
      @closed
    end

    def close
      @closed = true
    end

    def connack
      MQTT::Packet::Connack.new(:return_code => 0, :session_present => @session_present)
    end

    def packets
      buffer = @written.dup
      packets = []
      until buffer.empty?
        packet = MQTT::Packet.parse_header(buffer)
        packet.parse_body(buffer.slice!(0, packet.body_length))
        packets << packet
      end
      packets
    end
  end

  let(:store) { MQTT::InMemorySessionStore.new }

  def new_client(session_present: true, clean_session: false)
    socket = SessionFakeSocket.new(session_present)
    client = MQTT::Client.new(
      :host => 'localhost',
      :client_id => 'persistent-client',
      :clean_session => clean_session,
      :session_store => store,
      :ack_timeout => 1,
      :keep_alive => 0
    )
    allow(TCPSocket).to receive(:new).and_return(socket)
    allow(Thread).to receive(:new)
    allow(client).to receive(:receive_connack).and_return(socket.connack)
    client.connect
    [client, socket]
  end

  def crash(client, socket)
    client.disconnect(false)
    socket.close
  end

  def packet_summary(socket)
    socket.packets.map do |packet|
      name = packet.class.name.split('::').last
      packet.respond_to?(:id) && packet.id != 0 ? "#{name}:#{packet.id}" : name
    end
  end

  def incoming_publish(id = 42)
    MQTT::Packet::Publish.new(
      :id => id,
      :qos => 2,
      :topic => 'topic',
      :payload => 'payload'
    )
  end

  def outgoing_publish(client, id = client.send(:next_packet_id))
    MQTT::Packet::Publish.new(
      :id => id,
      :qos => 2,
      :topic => 'topic',
      :payload => 'payload'
    )
  end

  it 'lists outgoing restart packets before PUBREC as CONNECT and PUBLISH(DUP)' do
    client, original = new_client
    publish = outgoing_publish(client, 1)
    client.send(:remember_outgoing_publish, publish)
    client.send(:send_packet, publish)
    crash(client, original)

    _restored, resumed = new_client

    expect(packet_summary(resumed)).to eq(['Connect', 'Publish:1'])
    expect(resumed.packets.last.duplicate).to be(true)
  end

  it 'lists outgoing restart packets after PUBREC as CONNECT and PUBREL only' do
    client, original = new_client
    client.send(:remember_outgoing_publish, outgoing_publish(client, 1))
    client.send(:handle_packet, MQTT::Packet::Pubrec.new(:id => 1))
    crash(client, original)

    _restored, resumed = new_client

    expect(packet_summary(resumed)).to eq(['Connect', 'Pubrel:1'])
  end

  it 'lists PUBREL after a restart between PUBREC persistence and its write' do
    client, original = new_client
    client.send(:remember_outgoing_publish, outgoing_publish(client, 1))
    allow(client).to receive(:send_packet).and_wrap_original do |send_packet, packet, *args, &block|
      raise MQTT::NotConnectedException if packet.is_a?(MQTT::Packet::Pubrel)

      send_packet.call(packet, *args, &block)
    end
    expect do
      client.send(:handle_packet, MQTT::Packet::Pubrec.new(:id => 1))
    end.to raise_error(MQTT::NotConnectedException)
    original.close

    _restored, resumed = new_client

    expect(packet_summary(resumed)).to eq(['Connect', 'Pubrel:1'])
  end

  it 'lists only CONNECT after outgoing PUBCOMP' do
    client, original = new_client
    client.send(:remember_outgoing_publish, outgoing_publish(client, 1))
    client.send(:handle_packet, MQTT::Packet::Pubrec.new(:id => 1))
    client.send(:handle_packet, MQTT::Packet::Pubcomp.new(:id => 1))
    crash(client, original)

    _restored, resumed = new_client

    expect(packet_summary(resumed)).to eq(['Connect'])
  end

  it 'lists incoming restart packets before PUBREC as CONNECT then PUBREC after consume' do
    client, original = new_client
    client.send(:handle_packet, incoming_publish)
    crash(client, original)

    restored, resumed = new_client
    packet = restored.get_packet

    expect(packet.id).to eq(42)
    expect(packet.payload).to eq('payload')
    expect(packet_summary(resumed)).to eq(['Connect', 'Pubrec:42'])
  end

  it 'lists incoming restart packets after PUBREL as CONNECT and PUBCOMP' do
    client, original = new_client
    client.send(:handle_packet, incoming_publish)
    client.get_packet
    allow(client).to receive(:send_packet).and_raise(MQTT::NotConnectedException)
    expect do
      client.send(:handle_packet, MQTT::Packet::Pubrel.new(:id => 42))
    end.to raise_error(MQTT::NotConnectedException)
    original.close

    _restored, resumed = new_client

    expect(packet_summary(resumed)).to eq(['Connect', 'Pubcomp:42'])
  end

  it 'lists CONNECT and PUBREC after restart following PUBREC' do
    client, original = new_client
    client.send(:handle_packet, incoming_publish)
    client.get_packet
    crash(client, original)

    restored, resumed = new_client

    expect(packet_summary(resumed)).to eq(['Connect', 'Pubrec:42'])
    expect(restored.queue_length).to eq(0)

    duplicate = incoming_publish
    duplicate.duplicate = true
    restored.send(:handle_packet, duplicate)

    expect(packet_summary(resumed)).to eq(['Connect', 'Pubrec:42', 'Pubrec:42'])
    expect(restored.queue_length).to eq(0)
  end

  it 'does not redeliver to the application after delivery before PUBREC is written' do
    client, original = new_client
    client.send(:handle_packet, incoming_publish)
    client.send(:prepare_received_publish, incoming_publish)
    crash(client, original)

    restored, resumed = new_client

    expect(restored.queue_length).to eq(0)
    expect(packet_summary(resumed)).to eq(['Connect', 'Pubrec:42'])
  end

  it 'does not redeliver to the application after PUBREC is written' do
    client, original = new_client
    client.send(:handle_packet, incoming_publish)
    client.get_packet
    expect(packet_summary(original)).to eq(['Connect', 'Pubrec:42'])
    crash(client, original)

    restored, resumed = new_client

    expect(restored.queue_length).to eq(0)
    expect(packet_summary(resumed)).to eq(['Connect', 'Pubrec:42'])
  end

  it 'lists only CONNECT after incoming PUBCOMP' do
    client, original = new_client
    client.send(:handle_packet, incoming_publish)
    client.get_packet
    client.send(:handle_packet, MQTT::Packet::Pubrel.new(:id => 42))
    crash(client, original)

    _restored, resumed = new_client

    expect(packet_summary(resumed)).to eq(['Connect'])
  end

  it 'does not give a duplicate PUBREL to the application' do
    client, socket = new_client
    client.send(:handle_packet, incoming_publish)
    packet = client.get_packet
    client.send(:handle_packet, MQTT::Packet::Pubrel.new(:id => packet.id))
    client.send(:handle_packet, MQTT::Packet::Pubrel.new(:id => packet.id))

    expect(client.queue_length).to eq(0)
    expect(socket.packets.grep(MQTT::Packet::Publish)).to be_empty
    expect(packet_summary(socket)).to eq(['Connect', 'Pubrec:42', 'Pubcomp:42', 'Pubcomp:42'])
  end

  it 'uses separate outgoing and incoming packet identifier spaces' do
    client, socket = new_client
    client.send(:handle_packet, incoming_publish(1))
    incoming = client.get_packet
    outgoing = MQTT::Packet::Publish.new(
      :id => client.send(:next_packet_id),
      :qos => 2,
      :topic => 'outgoing',
      :payload => 'payload'
    )
    client.send(:remember_outgoing_publish, outgoing)
    client.send(:send_packet, outgoing)
    client.send(:handle_packet, MQTT::Packet::Pubrec.new(:id => outgoing.id))
    client.send(:handle_packet, MQTT::Packet::Pubrel.new(:id => incoming.id))

    expect(packet_summary(socket)).to eq([
      'Connect', 'Pubrec:1', 'Publish:1', 'Pubrel:1', 'Pubcomp:1'
    ])
  end

  it 'resets persisted state for Clean Session' do
    client, original = new_client
    client.send(:handle_packet, incoming_publish)
    crash(client, original)

    clean, socket = new_client(:session_present => false, :clean_session => true)

    expect(store.load_session('persistent-client')).to be_nil
    expect(clean.queue_length).to eq(0)
    expect(packet_summary(socket)).to eq(['Connect'])
    clean.disconnect
  end

  it 'discards local state when persistent CONNACK reports no session' do
    client, original = new_client
    client.send(:remember_outgoing_publish, outgoing_publish(client, 1))
    crash(client, original)

    resumed, socket = new_client(:session_present => false)

    expect(store.load_session('persistent-client')).to be_nil
    expect(packet_summary(socket)).to eq(['Connect'])
    resumed.disconnect
  end

  it 'discards persistent state after a graceful DISCONNECT' do
    client, socket = new_client
    client.send(:remember_outgoing_publish, outgoing_publish(client, 1))
    client.disconnect

    expect(store.load_session('persistent-client')).to be_nil
    expect(packet_summary(socket)).to include('Disconnect')
  end
end

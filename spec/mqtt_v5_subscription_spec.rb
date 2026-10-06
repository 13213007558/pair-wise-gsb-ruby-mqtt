# frozen_string_literal: true

$:.unshift(File.dirname(__FILE__))

require 'spec_helper'
require 'mqtt'

describe 'MQTT 5 subscription identifiers' do
  let(:client) { MQTT::Client.new(:host => 'localhost', :version => '5.0') }
  let(:socket) { StringIO.new(''.b) }

  before(:each) do
    client.instance_variable_set(:@socket, socket)
  end

  describe 'anonymous subscriptions (no identifier)' do
    it 'tracks filters locally and matches by topic even without packet identifiers' do
      packet_id = client.subscribe('a/+')
      read_sent_packet
      suback_for(MQTT::Packet::Subscribe.new(:id => packet_id), [0])

      # No subscription identifier in the PUBLISH packet
      packet = publish_packet('a/x', 'hi')
      message = client.send(:build_received_message, packet)
      expect(message.identifiers).to eq([])
      expect(message.matches.size).to eq(1)
      expect(message.matches.first.filter).to eq('a/+')
      expect(message.matches.first.identifier).to be_nil
    end

    it 'still reports one message when anonymous filters overlap' do
      first = client.subscribe('a/+')
      read_sent_packet
      suback_for(MQTT::Packet::Subscribe.new(:id => first), [0])
      second = client.subscribe('a/#')
      read_sent_packet
      suback_for(MQTT::Packet::Subscribe.new(:id => second), [0])

      packet = publish_packet('a/x', 'hi')
      message = client.send(:build_received_message, packet)
      expect(message.matches.map(&:filter)).to match_array(['a/+', 'a/#'])
      expect(message.matches.map(&:subscription).uniq.size).to eq(2)
    end

    it 'removes an anonymous subscription on unsubscribe' do
      packet_id = client.subscribe('a/+')
      read_sent_packet
      suback_for(MQTT::Packet::Subscribe.new(:id => packet_id), [0])

      unsubscribe_id = client.unsubscribe('a/+')
      read_sent_packet
      unsuback_for(MQTT::Packet::Unsubscribe.new(:id => unsubscribe_id), [0x00])

      expect(client.subscriptions).to be_empty
      message = client.send(:build_received_message, publish_packet('a/x'))
      expect(message.matches).to be_empty
    end
  end

  def read_sent_packet
    raw = socket.string.dup
    socket.truncate(0)
    socket.rewind
    MQTT::Packet.parse(raw, '5.0')
  end

  def receive_packet(packet)
    client.send(:handle_packet, packet)
  end

  def publish_packet(topic, payload = 'payload', identifiers = nil, qos: 0)
    properties = {}
    properties[:subscription_identifier] = identifiers unless identifiers.nil?
    MQTT::Packet::Publish.new(
      :topic => topic,
      :payload => payload,
      :qos => qos,
      :version => '5.0',
      :properties => properties
    )
  end

  def suback_for(packet, codes)
    receive_packet(MQTT::Packet::Suback.new(
      :id => packet.id,
      :return_codes => codes,
      :version => '5.0'
    ))
  end

  def unsuback_for(packet, codes = [])
    receive_packet(MQTT::Packet::Unsuback.new(
      :id => packet.id,
      :reason_codes => codes,
      :version => '5.0'
    ))
  end

  describe 'sending SUBSCRIBE packets' do
    it 'encodes a subscription identifier as a property' do
      client.subscribe('a/b', :identifier => 42)
      packet = read_sent_packet
      expect(packet).to be_a(MQTT::Packet::Subscribe)
      expect(packet.subscription_identifier).to eq(42)
      expect(packet.topic_filters).to eq(['a/b'])
    end

    it 'rejects subscription identifiers on MQTT 3.1.1 clients' do
      old_client = MQTT::Client.new(:host => 'localhost', :version => '3.1.1')
      old_client.instance_variable_set(:@socket, socket)
      expect {
        old_client.subscribe('a/b', :identifier => 1)
      }.to raise_error(ArgumentError, /MQTT version 5.0/)
    end

    it 'rejects subscription identifiers outside the valid range' do
      expect {
        client.subscribe('a/b', :identifier => 0)
      }.to raise_error(ArgumentError, /between 1 and 268435455/)
    end

    it 'does not add the property when no identifier is given' do
      client.subscribe('a/b')
      packet = read_sent_packet
      expect(packet.subscription_identifier).to be_nil
      expect(packet.properties).to eq({})
    end

    it 'tracks subscriptions on MQTT 3.1.1 using plain SUBACK granted QoS bytes' do
      old_client = MQTT::Client.new(:host => 'localhost', :version => '3.1.1')
      old_socket = StringIO.new(''.b)
      old_client.instance_variable_set(:@socket, old_socket)
      packet_id = old_client.subscribe('a/+')
      old_socket.truncate(0)
      old_client.send(:handle_packet, MQTT::Packet::Suback.new(
        :id => packet_id,
        :return_codes => [2],
        :version => '3.1.1'
      ))
      expect(old_client.subscriptions.size).to eq(1)
      expect(old_client.subscriptions.first.filters).to eq('a/+' => 2)
    end
  end

  describe 'SUBACK handling' do
    it 'activates filters that were granted a QoS' do
      packet_id = client.subscribe('a/+', 'b/#', :identifier => 7)
      read_sent_packet
      suback_for(MQTT::Packet::Subscribe.new(:id => packet_id), [0, 1])

      subscription = client.subscription(7)
      expect(subscription.filters).to eq('a/+' => 0, 'b/#' => 1)
    end

    it 'does not activate a filter when the SUBACK failed for that filter' do
      packet_id = client.subscribe('a/+', 'b/#', :identifier => 7)
      read_sent_packet
      suback_for(MQTT::Packet::Subscribe.new(:id => packet_id), [0, 0x80])

      subscription = client.subscription(7)
      expect(subscription.filters).to eq('a/+' => 0)
    end

    it 'removes the logical subscription when every filter failed' do
      packet_id = client.subscribe('a/+', :identifier => 7)
      read_sent_packet
      suback_for(MQTT::Packet::Subscribe.new(:id => packet_id), [0x80])
      expect(client.subscription(7)).to be_nil
      expect(client.subscriptions).to be_empty
    end

    it 'merges filters into the same logical subscription when an identifier is reused' do
      first = client.subscribe('a/+', :identifier => 7)
      read_sent_packet
      suback_for(MQTT::Packet::Subscribe.new(:id => first), [0])
      second = client.subscribe('b/+', :identifier => 7)
      read_sent_packet
      suback_for(MQTT::Packet::Subscribe.new(:id => second), [1])

      expect(client.subscriptions.size).to eq(1)
      expect(client.subscription(7).filters).to eq('a/+' => 0, 'b/+' => 1)
    end
  end

  describe 'receiving PUBLISH packets' do
    before(:each) do
      packet_id = client.subscribe('a/+', 'b/#', :identifier => 7)
      read_sent_packet
      suback_for(MQTT::Packet::Subscribe.new(:id => packet_id), [0, 0])
    end

    it 'keeps all subscription identifiers from the PUBLISH packet' do
      packet = publish_packet('a/x', 'hi', [7, 99])
      message = client.send(:build_received_message, packet)
      expect(message.identifiers).to eq([7, 99])
      expect(message.topic).to eq('a/x')
      expect(message.payload).to eq('hi')
    end

    it 'forms one message and lists every matching filter' do
      packet_id = client.subscribe('a/#', :identifier => 8)
      read_sent_packet
      suback_for(MQTT::Packet::Subscribe.new(:id => packet_id), [0])

      packet = publish_packet('a/x', 'hi', [7, 8])
      message = client.send(:build_received_message, packet)

      expect(message.matches.map(&:filter)).to match_array(['a/+', 'a/#'])
      expect(message.matches.map(&:identifier)).to match_array([7, 8])
    end

    it 'does not list a filter that does not match the topic' do
      packet = publish_packet('c/x', 'hi', [7])
      message = client.send(:build_received_message, packet)
      expect(message.matches).to be_empty
    end

    it 'puts a single event on the queues for a packet matching several filters' do
      packet_id = client.subscribe('a/#', :identifier => 8)
      read_sent_packet
      suback_for(MQTT::Packet::Subscribe.new(:id => packet_id), [0])

      receive_packet(publish_packet('a/x', 'hi', [7, 8]))
      expect(client.queue_length).to eq(1)
      expect(client.instance_variable_get(:@message_queue).length).to eq(1)
    end

    it 'matches shared subscription filters without using the group name' do
      packet_id = client.subscribe('$share/group1/c/+', :identifier => 9)
      read_sent_packet
      suback_for(MQTT::Packet::Subscribe.new(:id => packet_id), [0])

      packet = publish_packet('c/x', 'hi', [9])
      message = client.send(:build_received_message, packet)
      expect(message.matches.map(&:filter)).to eq(['$share/group1/c/+'])
    end
  end

  describe 'dispatch to logical subscriptions' do
    it 'calls the handler once when several filters of one subscription match' do
      received = []
      client.on_message('a/+', 'a/#', :identifier => 3) do |message|
        received << message
      end
      pending_id = client.instance_variable_get(:@pending_subscribes).keys.last
      read_sent_packet
      suback_for(MQTT::Packet::Subscribe.new(:id => pending_id), [0, 0])

      receive_packet(publish_packet('a/x', 'hi', [3]))
      expect(received.size).to eq(1)
      expect(received.first.matches.map(&:filter)).to match_array(['a/+', 'a/#'])
    end

    it 'dispatches one message object to every matching logical subscription' do
      seen = {}
      client.on_message('a/+', :identifier => 1) { |m| (seen[:one] ||= []) << m }
      read_sent_packet
      client.on_message('a/#', :identifier => 2) { |m| (seen[:two] ||= []) << m }
      read_sent_packet
      pending_ids = client.instance_variable_get(:@pending_subscribes).keys
      suback_for(MQTT::Packet::Subscribe.new(:id => pending_ids[0]), [0])
      suback_for(MQTT::Packet::Subscribe.new(:id => pending_ids[1]), [0])

      packet = publish_packet('a/x', 'hi', [1, 2])
      receive_packet(packet)
      expect(seen[:one].size).to eq(1)
      expect(seen[:two].size).to eq(1)
      expect(seen[:one].first).to equal(seen[:two].first)
    end

    it 'yields the same single message via get_message' do
      packet_id = client.subscribe('a/+', 'a/#', :identifier => 3)
      read_sent_packet
      suback_for(MQTT::Packet::Subscribe.new(:id => packet_id), [0, 0])

      receive_packet(publish_packet('a/x', 'hi', [3]))
      message = client.get_message
      expect(message.topic).to eq('a/x')
      expect(message.matches.map(&:filter)).to match_array(['a/+', 'a/#'])
    end
  end

  describe 'UNSUBSCRIBE handling' do
    before(:each) do
      packet_id = client.subscribe('a/+', 'b/+', :identifier => 7)
      read_sent_packet
      suback_for(MQTT::Packet::Subscribe.new(:id => packet_id), [0, 0])
    end

    it 'removes the mapping on a successful UNSUBACK' do
      packet_id = client.unsubscribe('a/+', :identifier => 7)
      read_sent_packet
      unsuback_for(MQTT::Packet::Unsubscribe.new(:id => packet_id), [0x00])

      expect(client.subscription(7).filters).to eq('b/+' => 0)

      packet = publish_packet('a/x', 'hi', [7])
      message = client.send(:build_received_message, packet)
      expect(message.matches).to be_empty
    end

    it 'keeps the mapping when the UNSUBACK reports a failure' do
      packet_id = client.unsubscribe('a/+', :identifier => 7)
      read_sent_packet
      unsuback_for(MQTT::Packet::Unsubscribe.new(:id => packet_id), [0x83])

      expect(client.subscription(7).filters).to eq('a/+' => 0, 'b/+' => 0)
    end

    it 'removes the whole logical subscription when its last filter is removed' do
      packet_id = client.unsubscribe('a/+', 'b/+', :identifier => 7)
      read_sent_packet
      unsuback_for(MQTT::Packet::Unsubscribe.new(:id => packet_id), [0x00, 0x00])
      expect(client.subscription(7)).to be_nil
    end

    it 'removes a filter from every logical subscription when no identifier is given' do
      first = client.subscribe('c/+', :identifier => 10)
      read_sent_packet
      suback_for(MQTT::Packet::Subscribe.new(:id => first), [0])
      second = client.subscribe('c/+', :identifier => 11)
      read_sent_packet
      suback_for(MQTT::Packet::Subscribe.new(:id => second), [0])

      packet = publish_packet('c/x', 'hi', [10, 11])
      message = client.send(:build_received_message, packet)
      expect(message.matches.map(&:identifier)).to match_array([10, 11])

      unsubscribe_id = client.unsubscribe('c/+')
      read_sent_packet
      unsuback_for(MQTT::Packet::Unsubscribe.new(:id => unsubscribe_id), [0x00])
      expect(client.subscription(10)).to be_nil
      expect(client.subscription(11)).to be_nil
      expect(client.subscription(7).filters).to eq('a/+' => 0, 'b/+' => 0)
    end
  end
end

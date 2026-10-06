# encoding: BINARY

require 'spec_helper'
require 'mqtt'

describe MQTT::Client, 'MQTT version 5 subscription identifiers' do
  let(:socket) do
    socket = StringIO.new
    socket.set_encoding('binary') if socket.respond_to?(:set_encoding)
    socket
  end

  let(:client) { MQTT::Client.new(:host => 'localhost', :version => '5.0') }

  before(:each) do
    client.instance_variable_set('@socket', socket)
  end

  def publish_packet(topic, payload, identifiers = [])
    properties = identifiers.empty? ? {} : { :subscription_identifier => identifiers }
    MQTT::Packet::Publish.new(
      :version => '5.0',
      :topic => topic,
      :payload => payload,
      :properties => properties
    )
  end

  def suback(id, codes)
    MQTT::Packet::Suback.new(:version => '5.0', :id => id, :return_codes => codes)
  end

  describe 'subscribing with an identifier' do
    it 'writes the subscription identifier property to the SUBSCRIBE packet' do
      client.subscribe('sport/+', :identifier => 7)
      expect(socket.string).to eq(
        "\x82\x0f" +
          "\x00\x01" +
          "\x02\x0b\x07" +
          "\x00\x07sport/+\x00"
      )
    end

    it 'raises an ArgumentError when used with MQTT 3.1.1' do
      old_client = MQTT::Client.new(:host => 'localhost')
      old_client.instance_variable_set('@socket', socket)
      expect {
        old_client.subscribe('sport/+', :identifier => 7)
      }.to raise_error(ArgumentError, /MQTT version 5/)
    end
  end

  describe 'SUBACK handling' do
    it 'registers successful subscriptions under their identifier' do
      client.subscribe('sport/+', 'news/+', :identifier => 7)
      client.send(:handle_packet, suback(1, [0, 1]))

      subscriptions = client.instance_variable_get('@subscriptions')
      expect(subscriptions.keys.sort).to eq(['news/+', 'sport/+'])
      expect(subscriptions['sport/+'].identifier).to eq(7)
      expect(subscriptions['sport/+'].qos).to eq(0)
      expect(subscriptions['news/+'].qos).to eq(1)
      expect(client.instance_variable_get('@subscription_identifiers')[7])
        .to match_array(['sport/+', 'news/+'])
    end

    it 'does not register filters whose SUBACK return code is a failure' do
      client.subscribe('sport/+', 'news/+', :identifier => 7)
      client.send(:handle_packet, suback(1, [0, 0x8f]))

      subscriptions = client.instance_variable_get('@subscriptions')
      expect(subscriptions.keys).to eq(['sport/+'])
      expect(client.instance_variable_get('@subscription_identifiers')[7])
        .to eq(['sport/+'])
    end

    it 'removes the pending entry once the SUBACK has been processed' do
      client.subscribe('sport/+', :identifier => 7)
      expect(client.instance_variable_get('@pending_subscribes')).to have_key(1)
      client.send(:handle_packet, suback(1, [0]))
      expect(client.instance_variable_get('@pending_subscribes')).not_to have_key(1)
    end

    it 'ignores a SUBACK that does not correspond to a pending subscribe' do
      expect {
        client.send(:handle_packet, suback(999, [0]))
      }.not_to raise_error
      expect(client.instance_variable_get('@subscriptions')).to be_empty
    end
  end

  describe 'matched_subscriptions' do
    before(:each) do
      client.subscribe('sport/+', 'sport/#', :identifier => 7)
      client.send(:handle_packet, suback(1, [0, 1]))
      client.subscribe('other/+', :identifier => 8)
      client.send(:handle_packet, suback(2, [0]))
    end

    it 'lists every matching filter once, even when several filters match' do
      packet = publish_packet('sport/tennis', 'win', [7])
      matched = client.matched_subscriptions(packet)
      expect(matched.map(&:filter)).to match_array(['sport/+', 'sport/#'])
    end

    it 'does not list subscriptions for other identifiers' do
      packet = publish_packet('other/place', 'win', [99])
      expect(client.matched_subscriptions(packet)).to eq([])
    end

    it 'expands all identifiers returned in a single packet' do
      client.subscribe('sport/tennis', :identifier => 9)
      client.send(:handle_packet, suback(3, [0]))
      packet = publish_packet('sport/tennis', 'win', [7, 8])
      expect(client.matched_subscriptions(packet).map(&:filter))
        .to match_array(['sport/+', 'sport/#', 'other/+'])

      packet = publish_packet('sport/tennis', 'win', [7, 9])
      expect(client.matched_subscriptions(packet).map(&:filter))
        .to match_array(['sport/+', 'sport/#', 'sport/tennis'])
    end

    it 'falls back to local filter matching when no identifier is present' do
      client.subscribe('local/+')
      client.send(:handle_packet, suback(3, [0]))
      packet = publish_packet('local/x', 'm')
      matched = client.matched_subscriptions(packet)
      expect(matched.map(&:filter)).to eq(['local/+'])
    end

    it 'strips the shared subscription group before matching by filter' do
      client.subscribe('$share/team1/local/+')
      client.send(:handle_packet, suback(3, [0]))
      packet = publish_packet('local/x', 'm')
      matched = client.matched_subscriptions(packet)
      expect(matched.map(&:filter)).to eq(['$share/team1/local/+'])
      expect(matched.first.group).to eq('team1')
      expect(matched.first.shared?).to eq(true)
    end
  end

  describe 'each_message dispatch' do
    before(:each) do
      client.subscribe('sport/+', 'sport/#', :identifier => 7)
      client.send(:handle_packet, suback(1, [0, 1]))
    end

    it 'produces one event per packet with all matching subscriptions listed' do
      client.instance_variable_get('@read_queue').push(
        publish_packet('sport/tennis', 'win', [7])
      )

      events = []
      client.each_message do |topic, payload, subscriptions|
        events << [topic, payload, subscriptions.map(&:filter)]
        break
      end

      expect(events.length).to eq(1)
      expect(events[0][0..1]).to eq(['sport/tennis', 'win'])
      expect(events[0][2]).to match_array(['sport/+', 'sport/#'])
    end

    it 'returns one message with all matches via get_message' do
      client.instance_variable_get('@read_queue').push(
        publish_packet('sport/tennis', 'win', [7])
      )
      topic, payload, subscriptions = client.get_message
      expect(topic).to eq('sport/tennis')
      expect(payload).to eq('win')
      expect(subscriptions.map(&:filter)).to match_array(['sport/+', 'sport/#'])
    end
  end

  describe 'UNSUBSCRIBE handling' do
    before(:each) do
      client.subscribe('sport/+', 'news/+', :identifier => 7)
      client.send(:handle_packet, suback(1, [0, 1]))
    end

    it 'removes the filter and identifier mapping on success' do
      client.unsubscribe('sport/+')
      unsuback = MQTT::Packet::Unsuback.new(:version => '5.0', :id => 2, :reason_codes => [0])
      client.send(:handle_packet, unsuback)

      subscriptions = client.instance_variable_get('@subscriptions')
      identifiers = client.instance_variable_get('@subscription_identifiers')
      expect(subscriptions.keys).to eq(['news/+'])
      expect(identifiers[7]).to eq(['news/+'])
    end

    it 'removes the identifier when the last filter is unsubscribed' do
      client.unsubscribe('sport/+', 'news/+')
      unsuback = MQTT::Packet::Unsuback.new(
        :version => '5.0', :id => 2, :reason_codes => [0, 0]
      )
      client.send(:handle_packet, unsuback)

      subscriptions = client.instance_variable_get('@subscriptions')
      identifiers = client.instance_variable_get('@subscription_identifiers')
      expect(subscriptions).to be_empty
      expect(identifiers).not_to have_key(7)
    end

    it 'keeps the subscription when the UNSUBACK reason code is a failure' do
      client.unsubscribe('sport/+')
      unsuback = MQTT::Packet::Unsuback.new(:version => '5.0', :id => 2, :reason_codes => [0x8f])
      client.send(:handle_packet, unsuback)

      subscriptions = client.instance_variable_get('@subscriptions')
      expect(subscriptions.keys).to include('sport/+')
      expect(client.instance_variable_get('@subscription_identifiers')[7])
        .to match_array(['sport/+', 'news/+'])
    end

    it 'writes a MQTT 5 UNSUBSCRIBE packet with a property length' do
      socket.string = ''
      client.unsubscribe('sport/+')
      expect(socket.string).to eq("\xa2\x0c\x00\x02\x00\x00\x07sport/+")
    end
  end

  describe 'local packet sequences from the socket' do
    it 'keeps the identifiers when reading a PUBLISH from the socket' do
      allow(IO).to receive(:select).and_return([[socket], [], []])

      server_publish = MQTT::Packet::Publish.new(
        :version => '5.0',
        :topic => 'sport/tennis',
        :payload => 'win',
        :properties => { :subscription_identifier => [7] }
      )
      socket.write(server_publish.to_s)
      socket.rewind

      client.instance_variable_set('@pending_subscribes', 1 => [
        MQTT::Client::Subscription.new('sport/+', 1, nil, 7)
      ])
      client.send(:handle_packet, suback(1, [1]))
      client.send(:receive_packet)

      packet = client.instance_variable_get('@read_queue').pop
      expect(packet.subscription_identifiers).to eq([7])
      expect(client.matched_subscriptions(packet).map(&:filter)).to eq(['sport/+'])
    end
  end
end

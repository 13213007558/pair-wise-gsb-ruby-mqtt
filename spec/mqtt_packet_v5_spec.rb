# encoding: BINARY

$:.unshift(File.dirname(__FILE__))

require 'spec_helper'
require 'mqtt'

describe 'MQTT 5 packets' do
  describe MQTT::Packet::Connect do
    it 'encodes a minimal MQTT 5 CONNECT packet' do
      packet = MQTT::Packet::Connect.new(
        :version => '5.0',
        :client_id => 'myclient',
        :clean_session => true,
        :keep_alive => 15
      )
      parsed = MQTT::Packet.parse(packet.to_s)
      expect(parsed.version).to eq('5.0')
      expect(parsed.client_id).to eq('myclient')
      expect(parsed.protocol_level).to eq(5)
    end

    it 'round trips a will message with binary payload and properties' do
      packet = MQTT::Packet::Connect.new(
        :version => '5.0',
        :client_id => 'myclient',
        :will_topic => 'will/topic',
        :will_payload => 'binary \x00 payload',
        :will_qos => 1,
        :will_properties => { :content_type => 'text/plain' },
        :properties => { :session_expiry_interval => 60 }
      )
      parsed = MQTT::Packet.parse(packet.to_s)
      expect(parsed.will_topic).to eq('will/topic')
      expect(parsed.will_payload).to eq('binary \x00 payload')
      expect(parsed.will_properties[:content_type]).to eq('text/plain')
      expect(parsed.properties[:session_expiry_interval]).to eq(60)
    end
  end

  describe MQTT::Packet::Connack do
    it 'parses an MQTT 5 CONNACK with an empty properties section' do
      packet = MQTT::Packet.parse("\x20\x03\x00\x00\x00", '5.0')
      expect(packet.return_code).to eq(0)
      expect(packet.properties).to eq({})
    end

    it 'round trips MQTT 5 properties' do
      packet = MQTT::Packet::Connack.new(
        :version => '5.0',
        :return_code => 0,
        :properties => { :assigned_client_identifier => 'abc', :server_keep_alive => 30 }
      )
      parsed = MQTT::Packet.parse(packet.to_s, '5.0')
      expect(parsed.properties[:assigned_client_identifier]).to eq('abc')
      expect(parsed.properties[:server_keep_alive]).to eq(30)
    end
  end

  describe MQTT::Packet::Subscribe do
    it 'encodes the subscription identifier property' do
      packet = MQTT::Packet::Subscribe.new(
        :version => '5.0',
        :id => 10,
        :topics => [['a/b', 0]],
        :subscription_identifier => 268_435_455
      )
      raw = packet.to_s
      parsed = MQTT::Packet.parse(raw, '5.0')
      expect(parsed.subscription_identifier).to eq(268_435_455)
      expect(parsed.topic_filters).to eq(['a/b'])
    end

    it 'encodes subscription options in the filter byte' do
      packet = MQTT::Packet::Subscribe.new(
        :version => '5.0',
        :id => 10,
        :topics => { 'a/b' => { :qos => 1, :no_local => true, :retain_as_published => true } }
      )
      parsed = MQTT::Packet.parse(packet.to_s, '5.0')
      expect(parsed.requested_qos_levels).to eq([1])
      raw_options = parsed.topic_options.first[:raw_options]
      expect(raw_options & 0x04).to eq(0x04)
      expect(raw_options & 0x08).to eq(0x08)
    end

    it 'rejects an invalid subscription identifier' do
      packet = MQTT::Packet::Subscribe.new(:version => '5.0', :topics => ['a/b'])
      expect {
        packet.subscription_identifier = 0
      }.to raise_error(ArgumentError, /Subscription identifier/)
    end
  end

  describe MQTT::Packet::Suback do
    it 'round trips MQTT 5 reason codes and properties' do
      packet = MQTT::Packet::Suback.new(
        :version => '5.0',
        :id => 10,
        :return_codes => [0, 1, 0x80],
        :properties => { :reason_string => 'partial' }
      )
      parsed = MQTT::Packet.parse(packet.to_s, '5.0')
      expect(parsed.return_codes).to eq([0, 1, 0x80])
      expect(parsed.properties[:reason_string]).to eq('partial')
    end
  end

  describe MQTT::Packet::Unsubscribe do
    it 'round trips filters with an empty properties section' do
      packet = MQTT::Packet::Unsubscribe.new(
        :version => '5.0',
        :id => 3,
        :topics => ['a/b', 'c/d']
      )
      parsed = MQTT::Packet.parse(packet.to_s, '5.0')
      expect(parsed.topics).to eq(['a/b', 'c/d'])
      expect(parsed.properties).to eq({})
    end
  end

  describe MQTT::Packet::Unsuback do
    it 'round trips per-filter reason codes' do
      packet = MQTT::Packet::Unsuback.new(
        :version => '5.0',
        :id => 3,
        :reason_codes => [0x00, 0x11, 0x8F]
      )
      parsed = MQTT::Packet.parse(packet.to_s, '5.0')
      expect(parsed.reason_codes).to eq([0x00, 0x11, 0x8F])
    end
  end

  describe MQTT::Packet::Publish do
    it 'round trips multiple subscription identifiers' do
      packet = MQTT::Packet::Publish.new(
        :version => '5.0',
        :topic => 'a/b',
        :payload => 'hello',
        :subscription_identifiers => [1, 300]
      )
      parsed = MQTT::Packet.parse(packet.to_s, '5.0')
      expect(parsed.subscription_identifiers).to eq([1, 300])
      expect(parsed.payload).to eq('hello')
    end

    it 'parses user properties alongside the subscription identifier' do
      packet = MQTT::Packet::Publish.new(
        :version => '5.0',
        :topic => 'a/b',
        :payload => 'x',
        :properties => {
          :subscription_identifier => 5,
          :user_property => [['k1', 'v1'], ['k2', 'v2']],
          :content_type => 'text'
        }
      )
      parsed = MQTT::Packet.parse(packet.to_s, '5.0')
      expect(parsed.subscription_identifiers).to eq([5])
      expect(parsed.properties[:user_property]).to eq([['k1', 'v1'], ['k2', 'v2']])
      expect(parsed.properties[:content_type]).to eq('text')
    end
  end

  describe MQTT::Packet do
    it 'encodes and parses variable byte integers correctly' do
      packet = MQTT::Packet::Subscribe.new(
        :version => '5.0',
        :id => 1,
        :topics => ['a'],
        :subscription_identifier => 321
      )
      parsed = MQTT::Packet.parse(packet.to_s, '5.0')
      expect(parsed.subscription_identifier).to eq(321)
    end
  end
end

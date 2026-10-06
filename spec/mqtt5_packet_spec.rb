# encoding: BINARY
# Encoding is set to binary, so that the binary packets aren't validated as UTF-8

require 'spec_helper'
require 'mqtt'

describe MQTT::Packet, 'MQTT version 5' do
  describe MQTT::Packet::Connect do
    it 'should serialise a minimal MQTT 5 connect packet' do
      packet = MQTT::Packet::Connect.new(
        :version => '5.0',
        :client_id => 'myclient'
      )
      expect(packet.to_s).to eq(
        "\x10\x15" +
          "\x00\x04MQTT" +
          "\x05\x02\x00\x0f" +
          "\x00" +
          "\x00\x08myclient"
      )
    end

    it 'should serialise a connect packet with will properties' do
      packet = MQTT::Packet::Connect.new(
        :version => '5.0',
        :client_id => 'myclient',
        :will_topic => 'topic',
        :will_payload => 'hello',
        :will_qos => 1,
        :will_retain => true,
        :will_properties => { :content_type => 'text/plain' }
      )
      parsed = MQTT::Packet.parse(packet.to_s)
      expect(parsed.version).to eq('5.0')
      expect(parsed.will_topic).to eq('topic')
      expect(parsed.will_payload).to eq('hello')
      expect(parsed.will_qos).to eq(1)
      expect(parsed.will_retain).to eq(true)
      expect(parsed.will_properties[:content_type]).to eq('text/plain')
    end

    it 'should parse a MQTT 5 connect packet read from bytes' do
      packet = MQTT::Packet.parse(
        "\x10\x15" +
          "\x00\x04MQTT\x05\x02\x00\x0f\x00\x00\x08myclient"
      )
      expect(packet.version).to eq('5.0')
      expect(packet.protocol_level).to eq(5)
      expect(packet.client_id).to eq('myclient')
    end

    it 'should treat a version of 5 as 5.0' do
      packet = MQTT::Packet::Connect.new(:version => '5', :client_id => 'c')
      expect(packet.version).to eq('5.0')
    end
  end

  describe MQTT::Packet::Connack do
    it 'should serialise a minimal MQTT 5 connack packet' do
      packet = MQTT::Packet::Connack.new(:version => '5.0')
      expect(packet.to_s).to eq("\x20\x03\x00\x00\x00")
    end

    it 'should parse a MQTT 5 connack packet with a reason code' do
      packet = MQTT::Packet.parse("\x20\x03\x00\x19\x00", '5.0')
      expect(packet.version).to eq('5.0')
      expect(packet.return_code).to eq(0x19)
      expect(packet.reason_code).to eq(0x19)
    end

    it 'should preserve session present in a MQTT 5 connack packet' do
      packet = MQTT::Packet.parse("\x20\x03\x01\x00\x00", '5.0')
      expect(packet.session_present).to eq(true)
    end
  end

  describe MQTT::Packet::Subscribe do
    it 'should serialise a property length and subscription options' do
      packet = MQTT::Packet::Subscribe.new(
        :version => '5.0',
        :id => 0x1234,
        :topics => [['test', 1]]
      )
      expect(packet.to_s).to eq("\x82\x0a\x12\x34\x00\x00\x04test\x01")
    end

    it 'should serialise a subscription identifier' do
      packet = MQTT::Packet::Subscribe.new(
        :version => '5.0',
        :id => 0x1234,
        :topics => [['a/b', 0], ['c/d', 2]],
        :subscription_identifier => 10
      )
      expect(packet.to_s).to eq(
        "\x82\x11\x12\x34" +
          "\x02\x0b\x0a" +
          "\x00\x03a/b\x00" +
          "\x00\x03c/d\x02"
      )
    end

    it 'should serialise a subscription identifier requiring two bytes' do
      packet = MQTT::Packet::Subscribe.new(
        :version => '5.0',
        :id => 1,
        :topics => [['a/b', 0]],
        :subscription_identifier => 321
      )
      parsed = MQTT::Packet.parse(packet.to_s, '5.0')
      expect(parsed.subscription_identifier).to eq(321)
    end

    it 'should parse a packet with a subscription identifier' do
      packet = MQTT::Packet.parse(
        "\x82\x0c\x12\x34\x02\x0b\x0a\x00\x04test\x00",
        '5.0'
      )
      expect(packet.id).to eq(0x1234)
      expect(packet.subscription_identifier).to eq(10)
      expect(packet.topics).to eq([['test', 0]])
    end

    it 'should reject a subscription identifier of zero' do
      expect {
        MQTT::Packet::Subscribe.new(
          :version => '5.0',
          :id => 1,
          :topics => [['test', 0]],
          :subscription_identifier => 0
        )
      }.to raise_error(MQTT::ProtocolException)
    end

    it 'should reject a subscription identifier that is too large' do
      expect {
        MQTT::Packet::Subscribe.new(
          :version => '5.0',
          :id => 1,
          :topics => [['test', 0]],
          :subscription_identifier => 268_435_456
        )
      }.to raise_error(MQTT::ProtocolException)
    end
  end

  describe MQTT::Packet::Suback do
    it 'should serialise reason codes with a property length' do
      packet = MQTT::Packet::Suback.new(
        :version => '5.0',
        :id => 0x1234,
        :return_codes => [0, 2, 0x80]
      )
      expect(packet.to_s).to eq("\x90\x06\x12\x34\x00\x00\x02\x80")
    end

    it 'should parse reason codes, including failures' do
      packet = MQTT::Packet.parse("\x90\x05\x12\x34\x00\x01\x8f", '5.0')
      expect(packet.return_codes).to eq([1, 0x8f])
    end
  end

  describe MQTT::Packet::Unsubscribe do
    it 'should include a property length' do
      packet = MQTT::Packet::Unsubscribe.new(
        :version => '5.0',
        :id => 5,
        :topics => 'a/b'
      )
      expect(packet.to_s).to eq("\xa2\x08\x00\x05\x00\x00\x03a/b")
    end
  end

  describe MQTT::Packet::Unsuback do
    it 'should serialise per-filter reason codes' do
      packet = MQTT::Packet::Unsuback.new(
        :version => '5.0',
        :id => 0x1234,
        :reason_codes => [0, 0x11]
      )
      expect(packet.to_s).to eq("\xb0\x05\x12\x34\x00\x00\x11")
    end

    it 'should parse per-filter reason codes' do
      packet = MQTT::Packet.parse("\xb0\x05\x12\x34\x00\x00\x11", '5.0')
      expect(packet.reason_codes).to eq([0, 0x11])
    end
  end

  describe MQTT::Packet::Publish do
    it 'should preserve all subscription identifiers returned by the server' do
      wire = MQTT::Packet::Publish.new(
        :version => '5.0',
        :topic => 'sport',
        :payload => 'win',
        :properties => { :subscription_identifier => [10, 268_416] }
      ).to_s
      packet = MQTT::Packet.parse(wire, '5.0')
      expect(packet.topic).to eq('sport')
      expect(packet.payload).to eq('win')
      expect(packet.subscription_identifiers).to eq([10, 268_416])
    end

    it 'should return an empty array when there are no subscription identifiers' do
      wire = "\x30\x0c\x00\x05sport\x00\x03win"
      packet = MQTT::Packet.parse(wire, '5.0')
      expect(packet.subscription_identifiers).to eq([])
    end

    it 'should round trip subscription identifiers through serialisation' do
      packet = MQTT::Packet::Publish.new(
        :version => '5.0',
        :topic => 'a/b',
        :payload => 'm',
        :properties => { :subscription_identifier => [1, 268_435_455] }
      )
      parsed = MQTT::Packet.parse(packet.to_s, '5.0')
      expect(parsed.subscription_identifiers).to eq([1, 268_435_455])
    end

    it 'should raise an exception for an unknown property' do
      expect {
        MQTT::Packet.parse("\x30\x05\x00\x01a\x01\xff", '5.0')
      }.to raise_error(MQTT::ProtocolException, /Unknown MQTT 5 property/)
    end
  end

  describe MQTT::Packet::Puback do
    it 'should serialise minimally on success' do
      packet = MQTT::Packet::Puback.new(:version => '5.0', :id => 1)
      expect(packet.to_s).to eq("\x40\x02\x00\x01")
    end

    it 'should serialise a failure reason code' do
      packet = MQTT::Packet::Puback.new(
        :version => '5.0',
        :id => 1,
        :reason_code => 0x10
      )
      expect(packet.to_s).to eq("\x40\x04\x00\x01\x10\x00")
    end
  end

  describe MQTT::Packet::Disconnect do
    it 'should serialise minimally for MQTT 5' do
      packet = MQTT::Packet::Disconnect.new(:version => '5.0')
      expect(packet.to_s).to eq("\xe0\x02\x00\x00")
    end

    it 'should serialise a reason code' do
      packet = MQTT::Packet::Disconnect.new(:version => '5.0', :reason_code => 0x8e)
      expect(packet.to_s).to eq("\xe0\x02\x8e\x00")
    end
  end
end

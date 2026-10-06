# encoding: BINARY
# Encoding is set to binary, so that the binary packets aren't validated as UTF-8

$:.unshift(File.dirname(__FILE__))

require 'spec_helper'

# A scripted MQTT-SN gateway used to drive the WillSession.
# Every packet crosses the real MQTT::SN::Packet codec in both directions.
class FakeSnGateway
  attr_reader :sent

  def initialize
    @sent = []
    @inbox = []
    @script = {}
  end

  def send_packet
    lambda do |packet|
      bytes = packet.to_s
      @sent << bytes
      handler = @script[MQTT::SN::Packet.parse(bytes).class]
      handler && handler.call
    end
  end

  def recv_packet
    lambda do |_timeout|
      bytes = @inbox.shift
      bytes.nil? ? nil : MQTT::SN::Packet.parse(bytes)
    end
  end

  # Queue a packet to be delivered to the client
  def push(packet)
    @inbox << packet.to_s
  end

  # Register a handler run whenever the client sends a packet of a type
  def on(packet_class, &block)
    @script[packet_class] = block
  end

  # All packets received from the client, parsed back from the wire
  def received
    @sent.map { |bytes| MQTT::SN::Packet.parse(bytes) }
  end
end

describe MQTT::SN::WillSession do
  let(:gateway) { FakeSnGateway.new }
  let(:session) do
    MQTT::SN::WillSession.new(
      :topic => 'will/topic',
      :message => 'gone',
      :qos => 1,
      :retain => true,
      :timeout => 0.01,
      :send_packet => gateway.send_packet,
      :recv_packet => gateway.recv_packet
    )
  end

  def script_handshake
    gateway.on(MQTT::SN::Packet::Connect) { gateway.push(MQTT::SN::Packet::Willtopicreq.new) }
    gateway.on(MQTT::SN::Packet::Willtopic) { gateway.push(MQTT::SN::Packet::Willmsgreq.new) }
    gateway.on(MQTT::SN::Packet::Willmsg) { gateway.push(MQTT::SN::Packet::Connack.new(:return_code => 0)) }
  end

  def connect
    script_handshake
    session.connect(:client_id => 'rspec')
  end

  describe 'the initial handshake' do
    it 'negotiates the will between CONNECT and CONNACK' do
      connack = connect
      expect(connack.return_code).to eq(0)
      expect(session).to be_connected

      connect_packet, willtopic, willmsg = gateway.received
      expect(connect_packet.request_will).to be_truthy
      expect(willtopic.topic_name).to eq('will/topic')
      expect(willtopic.qos).to eq(1)
      expect(willtopic.retain).to be_truthy
      expect(willmsg.data).to eq('gone')
    end

    it 'retransmits will data when the gateway repeats a request' do
      gateway.on(MQTT::SN::Packet::Connect) do
        gateway.push(MQTT::SN::Packet::Willtopicreq.new)
        gateway.push(MQTT::SN::Packet::Willtopicreq.new)
      end
      gateway.on(MQTT::SN::Packet::Willtopic) { gateway.push(MQTT::SN::Packet::Willmsgreq.new) }
      gateway.on(MQTT::SN::Packet::Willmsg) { gateway.push(MQTT::SN::Packet::Connack.new(:return_code => 0)) }

      session.connect(:client_id => 'rspec')

      topics = gateway.received.grep(MQTT::SN::Packet::Willtopic)
      expect(topics.length).to eq(2)
      expect(topics.first.duplicate).to be_falsey
      expect(topics.last.duplicate).to be_truthy
      expect(topics.last.topic_name).to eq('will/topic')
    end
  end

  describe 'updating the will' do
    it 'confirms topic and message separately' do
      connect
      gateway.on(MQTT::SN::Packet::Willtopicupd) { gateway.push(MQTT::SN::Packet::Willtopicresp.new(:return_code => 0)) }
      gateway.on(MQTT::SN::Packet::Willmsgupd) { gateway.push(MQTT::SN::Packet::Willmsgresp.new(:return_code => 0)) }

      result = session.update(:topic => 'new/topic', :message => 'bye')
      expect(result.topic_status).to eq(:accepted)
      expect(result.message_status).to eq(:accepted)
      expect(result.success?).to be_truthy
    end

    it 'reports a rejected message phase without claiming success' do
      connect
      gateway.on(MQTT::SN::Packet::Willtopicupd) { gateway.push(MQTT::SN::Packet::Willtopicresp.new(:return_code => 0)) }
      gateway.on(MQTT::SN::Packet::Willmsgupd) { gateway.push(MQTT::SN::Packet::Willmsgresp.new(:return_code => 1)) }

      result = session.update(:topic => 'new/topic', :message => 'bye')
      expect(result.topic_status).to eq(:accepted)
      expect(result.message_status).to eq(:rejected)
      expect(result.success?).to be_falsey
      expect(session.message).to eq('gone')
    end

    it 'skips the message phase when the topic update is rejected' do
      connect
      gateway.on(MQTT::SN::Packet::Willtopicupd) { gateway.push(MQTT::SN::Packet::Willtopicresp.new(:return_code => 1)) }

      result = session.update(:topic => 'new/topic', :message => 'bye')
      expect(result.topic_status).to eq(:rejected)
      expect(result.message_status).to eq(:skipped)
      expect(result.success?).to be_falsey
      expect(gateway.received.grep(MQTT::SN::Packet::Willmsgupd)).to be_empty
    end

    it 'carries QoS and retain changes in the WILLTOPICUPD flags' do
      connect
      gateway.on(MQTT::SN::Packet::Willtopicupd) { gateway.push(MQTT::SN::Packet::Willtopicresp.new(:return_code => 0)) }

      result = session.update(:qos => 2, :retain => false)
      expect(result.topic_status).to eq(:accepted)
      expect(result.message_status).to eq(:skipped)

      upd = gateway.received.grep(MQTT::SN::Packet::Willtopicupd).last
      expect(upd.qos).to eq(2)
      expect(upd.retain).to be_falsey
      expect(upd.topic_name).to eq('will/topic')
    end

    it 'retransmits with the DUP flag when a response times out' do
      connect
      attempts = 0
      gateway.on(MQTT::SN::Packet::Willtopicupd) do
        attempts += 1
        gateway.push(MQTT::SN::Packet::Willtopicresp.new(:return_code => 0)) if attempts > 1
      end

      result = session.update(:topic => 'new/topic')
      expect(result.topic_status).to eq(:accepted)

      upds = gateway.received.grep(MQTT::SN::Packet::Willtopicupd)
      expect(upds.length).to eq(2)
      expect(upds.last.duplicate).to be_truthy
    end

    it 'discards late confirmations from a previous round' do
      connect
      gateway.on(MQTT::SN::Packet::Willtopicupd) do
        # A stale WILLMSGRESP must not satisfy the upcoming message phase
        gateway.push(MQTT::SN::Packet::Willmsgresp.new(:return_code => 0))
        gateway.push(MQTT::SN::Packet::Willtopicresp.new(:return_code => 0))
      end
      gateway.on(MQTT::SN::Packet::Willmsgupd) { gateway.push(MQTT::SN::Packet::Willmsgresp.new(:return_code => 0)) }

      result = session.update(:topic => 'new/topic', :message => 'bye')
      expect(result.topic_status).to eq(:accepted)
      expect(result.message_status).to eq(:accepted)
      expect(gateway.received.grep(MQTT::SN::Packet::Willmsgupd).length).to eq(1)
    end
  end

  describe 'clearing the will' do
    it 'sends an empty WILLTOPICUPD and skips the message phase' do
      connect
      gateway.on(MQTT::SN::Packet::Willtopicupd) { gateway.push(MQTT::SN::Packet::Willtopicresp.new(:return_code => 0)) }

      result = session.clear
      expect(result.topic_status).to eq(:accepted)
      expect(result.message_status).to eq(:skipped)
      expect(result.success?).to be_truthy

      # An empty WILLTOPICUPD is just the two-byte header on the wire
      expect(gateway.sent.last).to eq([0x02, 0x1a].pack('CC'))
      expect(session.will?).to be_falsey
    end
  end
end

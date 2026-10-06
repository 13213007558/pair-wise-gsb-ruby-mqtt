# encoding: BINARY

$:.unshift(File.dirname(__FILE__))

require 'spec_helper'

describe MQTT::SN::WillSession do
  let(:sent) { [] }
  let(:received) { [] }

  subject(:session) do
    described_class.new(
      :send_packet => proc { |binary| sent << MQTT::SN::Packet.parse(binary) },
      :receive_packet => proc { received.shift or raise Timeout::Error },
      :ack_timeout => 0.01,
      :retries => 1
     )
  end

  def queue_packets(*packets)
    packets.each { |packet| received << packet }
  end

  def connect_with_will
    queue_packets(
      MQTT::SN::Packet::Willtopicreq.new,
      MQTT::SN::Packet::Willmsgreq.new,
      "\x03\x05\x00".b
    )

    session.connect(
      :client_id => 'client',
      :topic => 'will/topic',
      :message => 'bye',
      :qos => 1,
      :retain => true
    )
  end

  it 'completes the initial CONNECT, WILLTOPIC, WILLMSG and CONNACK handshake' do
    result = connect_with_will

    expect(sent.map(&:class)).to eq([
      MQTT::SN::Packet::Connect,
      MQTT::SN::Packet::Willtopic,
      MQTT::SN::Packet::Willmsg
    ])
    expect(sent[0].request_will).to be(true)
    expect(sent[1].topic_name).to eq('will/topic')
    expect(sent[1].qos).to eq(1)
    expect(sent[1].retain).to be(true)
    expect(sent[1].duplicate).to be(false)
    expect(sent[2].data).to eq('bye')
    expect(result.connect_ack).to be_accepted
    expect(session.will.message).to eq('bye')
  end

  it 'rejects the negotiated will when CONNACK contains an error code' do
    queue_packets(
      MQTT::SN::Packet::Willtopicreq.new,
      MQTT::SN::Packet::Willmsgreq.new,
      MQTT::SN::Packet::Connack.new(:return_code => 3)
    )

    result = connect_with_will

    expect(result.connect_ack).to be_rejected
    expect(result.connect_ack.return_code).to eq(3)
    expect(result).not_to be_accepted
    expect(session).not_to be_connected
    expect(session.will).to be_nil
  end

  it 'resends the correct initial packet when a gateway request is duplicated' do
    queue_packets(
      MQTT::SN::Packet::Willtopicreq.new,
      MQTT::SN::Packet::Willtopicreq.new,
      MQTT::SN::Packet::Willmsgreq.new,
      MQTT::SN::Packet::Willmsgreq.new,
      MQTT::SN::Packet::Connack.new(:return_code => 0)
    )

    result = session.connect(
      :client_id => 'client',
      :topic => 'will/topic',
      :message => 'bye'
    )

    expect(sent.map(&:class)).to eq([
      MQTT::SN::Packet::Connect,
      MQTT::SN::Packet::Willtopic,
      MQTT::SN::Packet::Willtopic,
      MQTT::SN::Packet::Willmsg,
      MQTT::SN::Packet::Willmsg
    ])
    expect(sent.grep(MQTT::SN::Packet::Willtopic).map(&:duplicate)).to all(be(false))
    expect(result.connect_ack).to be_accepted
  end

  it 'returns separate acknowledgement states when only the message update is rejected' do
    connect_with_will
    sent.clear

    queue_packets(
      MQTT::SN::Packet::Willtopicresp.new(:return_code => 0),
      MQTT::SN::Packet::Willmsgresp.new(:return_code => 2)
    )

    result = session.replace_will(
      :topic => 'will/new',
      :message => 'new bye',
      :qos => 2,
      :retain => false
    )

    expect(sent.map(&:class)).to eq([
      MQTT::SN::Packet::Willtopicupd,
      MQTT::SN::Packet::Willmsgupd
    ])
    expect(sent[0].topic_name).to eq('will/new')
    expect(sent[0].qos).to eq(2)
    expect(sent[0].retain).to be(false)
    expect(sent[0].to_s).to eq("\x0B\x1A\x40will/new".b)
    expect(result.topic_ack).to be_accepted
    expect(result.message_ack).to be_rejected
    expect(result.message_ack.return_code).to eq(2)
    expect(result).not_to be_accepted
    expect(session.will.topic).to eq('will/new')
    expect(session.will.message).to eq('bye')
  end

  it 'clears the will with an empty WILLTOPICUPD packet' do
    connect_with_will
    sent.clear
    queue_packets(MQTT::SN::Packet::Willtopicresp.new(:return_code => 0))

    result = session.clear_will

    expect(sent.map(&:class)).to eq([MQTT::SN::Packet::Willtopicupd])
    expect(sent[0].to_s).to eq("\x02\x1A".b)
    expect(result.topic_ack).to be_accepted
    expect(result.message_ack).to be_skipped
    expect(session.will).to be_nil
  end

  it 'sets duplicate only on the retried topic update wire packet' do
    connect_with_will
    sent.clear
    received.clear

    result = session.update_will_topic(:topic => 'will/retry')

    expect(result.topic_ack.attempts).to eq(2)
    expect(sent.length).to eq(2)
    expect(sent[0].duplicate).to be(false)
    expect(sent[1].duplicate).to be(true)
    expect(sent[1].type_id).to eq(0x1A)
  end

  it 'retries a message update with the same message wire packet' do
    connect_with_will
    sent.clear
    received.clear

    result = session.update_will_message('changed')

    expect(result.message_ack.attempts).to eq(2)
    expect(sent.length).to eq(2)
    expect(sent.map(&:class)).to all(eq(MQTT::SN::Packet::Willmsgupd))
    expect(sent.map(&:to_s)).to all(eq("\x09\x1Cchanged".b))
    expect(session.will.message).to eq('bye')
  end

  it 'does not let a late topic acknowledgement satisfy a message update' do
    connect_with_will
    sent.clear
    queue_packets(
      MQTT::SN::Packet::Willtopicresp.new(:return_code => 0),
      MQTT::SN::Packet::Willmsgresp.new(:return_code => 0)
    )
    session.update_will_topic(:topic => 'will/second')

    result = session.update_will_message('second message')
    late_result = session.handle_packet(MQTT::SN::Packet::Willtopicresp.new(:return_code => 0))

    expect(late_result).to eq(:ignored)
    expect(result.message_ack).to be_accepted
  end
end

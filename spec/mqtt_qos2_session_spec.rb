# encoding: BINARY

$:.unshift(File.dirname(__FILE__))

require 'spec_helper'
require 'mqtt'

describe 'QoS 2 session persistence' do
  # A fake broker backed by a real socket pair. Everything the
  # client writes is collected so tests can list the exact packets
  # that were sent on the wire, and the test decides precisely
  # when (or if) each acknowledgement is sent back.
  class FakeBroker
    attr_reader :socket

    def initialize
      @socket, @broker = UNIXSocket.pair
      @received = []
      @semaphore = Mutex.new
      @open = true
      @reader = Thread.new do
        begin
          loop do
            packet = MQTT::Packet.read(@broker)
            @semaphore.synchronize { @received << packet }
            # Auto-acknowledge connection attempts
            send_packet(MQTT::Packet::Connack.new(:return_code => 0)) if packet.is_a?(MQTT::Packet::Connect)
          end
        rescue MQTT::ProtocolException, IOError, SystemCallError
          # The client went away
        end
      end
    end

    # Send a packet from the broker to the client
    def send_packet(packet)
      @broker.write(packet.to_s)
    end

    # All packets received from the client so far
    def received
      @semaphore.synchronize { @received.dup }
    end

    # Packets received from the client, excluding CONNECTs
    def received_after_connect
      received.reject { |packet| packet.is_a?(MQTT::Packet::Connect) }
    end

    # Wait until the broker has received at least +count+ packets
    # of the given class
    def wait_for(packet_class, count = 1, timeout = 5)
      deadline = Time.now + timeout
      until received.count { |packet| packet.is_a?(packet_class) } >= count
        raise "Timed out waiting for #{packet_class}" if Time.now > deadline

        sleep 0.01
      end
    end

    def close
      @broker.close unless @broker.closed?
      @socket.close unless @socket.closed?
      @reader.kill
    end
  end

  let(:store) { MQTT::SessionStore::Memory.new }
  let(:broker) { FakeBroker.new }

  after(:each) do
    broker.close
  end

  def new_client(clean_session)
    MQTT::Client.new(
      :host => 'localhost',
      :clean_session => clean_session,
      :client_id => 'qos2-test-client',
      :session_store => store
    )
  end

  def connect(client, broker)
    allow(TCPSocket).to receive(:new).and_return(broker.socket)
    client.connect
  end

  # Simulate a client process restart: the old process is gone,
  # only the injected session store survives
  def restart(client, clean_session)
    client.disconnect(false)
    broker.close
    new_broker = FakeBroker.new
    new_client = new_client(clean_session)
    connect(new_client, new_broker)
    [new_client, new_broker]
  end

  def publish_qos2(client, payload)
    Thread.new { client.publish('test/topic', payload, false, 2) }
  end

  def wait_until(timeout = 5)
    deadline = Time.now + timeout
    until yield
      raise 'Timed out waiting for condition' if Time.now > deadline

      sleep 0.01
    end
  end

  describe 'sender side' do
    it 're-sends the PUBLISH with DUP when the connection dropped before the PUBREC' do
      client = new_client(false)
      connect(client, broker)
      publisher = publish_qos2(client, 'break-before-pubrec')

      # --- breakpoint: PUBLISH sent, PUBREC never arrives ---
      broker.wait_for(MQTT::Packet::Publish)
      expect(store.outgoing_state(1)).to eq(:publish)

      client2, broker2 = restart(client, false)
      begin
        broker2.wait_for(MQTT::Packet::Publish)

        # The message is not published from scratch: the same
        # packet id is re-sent with the DUP flag set
        sent = broker2.received_after_connect
        expect(sent.map(&:class)).to eq([MQTT::Packet::Publish])
        expect(sent[0].id).to eq(1)
        expect(sent[0].duplicate).to eq(true)
        expect(sent[0].payload).to eq('break-before-pubrec')

        # The resumed handshake can be driven to completion
        broker2.send_packet(MQTT::Packet::Pubrec.new(:id => 1))
        broker2.wait_for(MQTT::Packet::Pubrel)
        broker2.send_packet(MQTT::Packet::Pubcomp.new(:id => 1))
        wait_until { store.outgoing_state(1).nil? }
      ensure
        publisher.kill
        client2.disconnect(false)
        broker2.close
      end
    end

    it 're-sends only the PUBREL when the connection dropped after the PUBREC' do
      client = new_client(false)
      connect(client, broker)
      publisher = publish_qos2(client, 'break-before-pubcomp')

      # Drive the handshake up to the PUBREL
      broker.wait_for(MQTT::Packet::Publish)
      broker.send_packet(MQTT::Packet::Pubrec.new(:id => 1))

      # --- breakpoint: PUBREL sent, PUBCOMP never arrives ---
      broker.wait_for(MQTT::Packet::Pubrel)
      expect(store.outgoing_state(1)).to eq(:pubrel)

      client2, broker2 = restart(client, false)
      begin
        broker2.wait_for(MQTT::Packet::Pubrel)

        # The QoS 2 message is not re-published from scratch:
        # the handshake continues with a lone PUBREL
        sent = broker2.received_after_connect
        expect(sent.map(&:class)).to eq([MQTT::Packet::Pubrel])
        expect(sent[0].id).to eq(1)

        broker2.send_packet(MQTT::Packet::Pubcomp.new(:id => 1))
        wait_until { store.outgoing_state(1).nil? }
      ensure
        publisher.kill
        client2.disconnect(false)
        broker2.close
      end
    end

    it 'sends nothing after recovery when the handshake completed before the restart' do
      client = new_client(false)
      connect(client, broker)
      publisher = publish_qos2(client, 'no-break')

      broker.wait_for(MQTT::Packet::Publish)
      broker.send_packet(MQTT::Packet::Pubrec.new(:id => 1))
      broker.wait_for(MQTT::Packet::Pubrel)
      broker.send_packet(MQTT::Packet::Pubcomp.new(:id => 1))

      expect(publisher.value).not_to eq(-1)
      expect(store.outgoing_state(1)).to be_nil

      client2, broker2 = restart(client, false)
      begin
        sleep 0.2
        expect(broker2.received_after_connect).to be_empty
      ensure
        client2.disconnect(false)
        broker2.close
      end
    end
  end

  describe 'receiver side' do
    it 'delivers an incoming QoS 2 message exactly once across a restart' do
      client = new_client(false)
      connect(client, broker)

      incoming = MQTT::Packet::Publish.new(
        :id => 42, :qos => 2, :topic => 'test/incoming', :payload => 'exactly-once'
      )
      broker.send_packet(incoming)

      # --- breakpoint: PUBREC sent, PUBREL never arrives ---
      broker.wait_for(MQTT::Packet::Pubrec)
      expect(store.incoming_state(42)).to eq(:pubrec)
      expect(client.queue_empty?).to be_truthy

      client2, broker2 = restart(client, false)
      begin
        # The broker re-sends the PUBLISH with DUP after reconnect:
        # it is acknowledged again but not delivered yet
        incoming.duplicate = true
        broker2.send_packet(incoming)
        broker2.wait_for(MQTT::Packet::Pubrec)
        expect(client2.queue_empty?).to be_truthy

        # The first PUBREL completes the handshake and delivers
        broker2.send_packet(MQTT::Packet::Pubrel.new(:id => 42))
        broker2.wait_for(MQTT::Packet::Pubcomp)
        topic, payload = client2.get
        expect([topic, payload]).to eq(['test/incoming', 'exactly-once'])

        # A duplicate PUBREL is completed but not delivered again
        broker2.send_packet(MQTT::Packet::Pubrel.new(:id => 42))
        broker2.wait_for(MQTT::Packet::Pubcomp, 2)
        sleep 0.1
        expect(client2.queue_empty?).to be_truthy
        expect(store.incoming_state(42)).to eq(:pubcomp)
      ensure
        client2.disconnect(false)
        broker2.close
      end
    end
  end

  describe 'packet identifier spaces' do
    it 'tracks outgoing and incoming packet ids independently' do
      client = new_client(false)
      connect(client, broker)
      publisher = publish_qos2(client, 'outgoing')

      # Outgoing exchange uses packet id 1
      broker.wait_for(MQTT::Packet::Publish)

      # An incoming exchange may use the same id without clashes
      broker.send_packet(
        MQTT::Packet::Publish.new(:id => 1, :qos => 2, :topic => 'test/in', :payload => 'incoming')
      )
      broker.wait_for(MQTT::Packet::Pubrec)
      expect(store.outgoing_state(1)).to eq(:publish)
      expect(store.incoming_state(1)).to eq(:pubrec)

      # New outgoing packets skip ids of unfinished exchanges
      publisher2 = publish_qos2(client, 'second')
      broker.wait_for(MQTT::Packet::Publish, 2)
      second = broker.received_after_connect.select { |p| p.is_a?(MQTT::Packet::Publish) }.last
      expect(second.id).to eq(2)
    ensure
      publisher.kill
      publisher2.kill
      client.disconnect(false)
    end
  end

  describe 'clean session' do
    it 'discards pending handshake state when connecting with Clean Session set' do
      client = new_client(false)
      connect(client, broker)
      publisher = publish_qos2(client, 'reset-me')

      # Leave an outgoing exchange half-finished (PUBREL stage)
      broker.wait_for(MQTT::Packet::Publish)
      broker.send_packet(MQTT::Packet::Pubrec.new(:id => 1))
      broker.wait_for(MQTT::Packet::Pubrel)

      # Leave an incoming exchange half-finished (PUBREC stage)
      broker.send_packet(
        MQTT::Packet::Publish.new(:id => 7, :qos => 2, :topic => 'test/in', :payload => 'reset-me-too')
      )
      broker.wait_for(MQTT::Packet::Pubrec)
      expect(store.outgoing_state(1)).to eq(:pubrel)
      expect(store.incoming_state(7)).to eq(:pubrec)

      client2, broker2 = restart(client, true)
      begin
        # Nothing is re-sent and the store is empty
        sleep 0.2
        expect(broker2.received_after_connect).to be_empty
        expect(store.outgoing_state(1)).to be_nil
        expect(store.incoming_state(7)).to be_nil

        # The outgoing packet id space starts from scratch
        publisher2 = publish_qos2(client2, 'fresh')
        begin
          broker2.wait_for(MQTT::Packet::Publish)
          fresh = broker2.received_after_connect.find { |p| p.is_a?(MQTT::Packet::Publish) }
          expect(fresh.id).to eq(1)
          expect(fresh.duplicate).to eq(false)
        ensure
          publisher2.kill
        end
      ensure
        publisher.kill
        client2.disconnect(false)
        broker2.close
      end
    end
  end
end

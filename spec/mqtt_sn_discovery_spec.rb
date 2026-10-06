# encoding: BINARY

require 'spec_helper'

describe MQTT::SN::Discovery do
  class FakeClock
    attr_reader :now

    def initialize(start = 0)
      @now = start
    end

    def advance(seconds)
      @now += seconds
    end

    def call
      @now
    end
  end

  class FixedRandom
    def initialize(values = [])
      @values = values
    end

    def rand(range)
      @values.empty? ? range.first : @values.shift
    end
  end

  let(:clock) { FakeClock.new(1000) }
  let(:gateway_address) { '10.1.0.1:1883' }
  let(:other_address) { '10.1.0.2:1883' }
  let(:relay_address) { '10.2.0.9:5000' }

  let(:discovery) do
    MQTT::SN::Discovery.new(
      :clock => clock,
      :random => FixedRandom.new([3]),
      :response_delay => 1..5,
      :forward_delay => 1,
      :gateway_ttl => 100
    )
  end

  def advertise(gateway_id, duration)
    MQTT::SN::Packet::Advertise.new(
      :gateway_id => gateway_id,
      :duration => duration
    ).to_s
  end

  def searchgw(radius)
    MQTT::SN::Packet::Searchgw.new(:radius => radius).to_s
  end

  def gwinfo(gateway_id, gateway_address = nil)
    MQTT::SN::Packet::Gwinfo.new(
      :gateway_id => gateway_id,
      :gateway_address => gateway_address
    ).to_s
  end

  describe '#search!' do
    it 'returns a SEARCHGW broadcast carrying the configured radius' do
      datagram = discovery.search!
      packet = MQTT::SN::Packet.parse(datagram.first)

      expect(packet).to be_a(MQTT::SN::Packet::Searchgw)
      expect(packet.radius).to eq(1)
      expect(datagram.last).to eq(MQTT::SN::Discovery::BROADCAST_ADDRESS)
    end

    it 'suppresses repeated searches inside the minimum interval' do
      expect(discovery.search!).to_not be_nil
      clock.advance(5)
      expect(discovery.search!).to be_nil

      clock.advance(6)
      expect(discovery.search!).to_not be_nil
    end
  end

  describe 'receiving ADVERTISE' do
    it 'registers the sender as a direct candidate with a validity period' do
      discovery.receive(advertise(7, 30), gateway_address)

      gateway = discovery.select_gateway
      expect(gateway.gateway_id).to eq(7)
      expect(gateway.address).to eq(gateway_address)
      expect(gateway).to be_direct
      expect(gateway.forwarded_by).to be_nil
      expect(gateway.remaining(clock.now)).to eq(30)
    end

    it 'renews an existing gateway and updates its id from the same address' do
      discovery.receive(advertise(7, 30), gateway_address)
      clock.advance(10)
      discovery.receive(advertise(9, 30), gateway_address)

      expect(discovery.gateways.size).to eq(1)
      gateway = discovery.select_gateway
      expect(gateway.gateway_id).to eq(9)
      expect(gateway.last_seen).to eq(clock.now)
      expect(gateway.remaining(clock.now)).to eq(30)
    end

    it 'withdraws the gateway immediately when the duration is zero' do
      discovery.receive(advertise(7, 30), gateway_address)
      discovery.tick
      discovery.receive(advertise(7, 0), gateway_address)

      expect(discovery.gateways).to be_empty
      expect(discovery.select_gateway).to be_nil
    end

    it 'ignores a zero duration advertisement from an unknown sender' do
      expect(discovery.receive(advertise(7, 0), gateway_address)).to eq([])
      expect(discovery.gateways).to be_empty
    end
  end

  describe 'receiving GWINFO' do
    it 'treats a packet without an address as a direct report from its sender' do
      discovery.receive(gwinfo(4), gateway_address)

      gateway = discovery.select_gateway
      expect(gateway.gateway_id).to eq(4)
      expect(gateway.address).to eq(gateway_address)
      expect(gateway).to be_direct
    end

    it 'treats an embedded address matching the sender as direct' do
      discovery.receive(gwinfo(4, gateway_address), gateway_address)

      gateway = discovery.select_gateway
      expect(gateway.address).to eq(gateway_address)
      expect(gateway).to be_direct
    end

    it 'records the relay when the embedded address differs from the sender' do
      discovery.receive(gwinfo(4, gateway_address), relay_address)

      gateway = discovery.select_gateway
      expect(gateway.address).to eq(gateway_address)
      expect(gateway).to_not be_direct
      expect(gateway.forwarded_by).to eq(relay_address)
      expect(gateway.expired?(clock.now)).to be false
      clock.advance(100)
      expect(gateway.expired?(clock.now)).to be true
    end

    it 'promotes a forwarded candidate when the gateway later reports itself' do
      discovery.receive(gwinfo(4, gateway_address), relay_address)
      discovery.receive(advertise(5, 20), gateway_address)

      gateway = discovery.select_gateway
      expect(gateway.gateway_id).to eq(5)
      expect(gateway).to be_direct
      expect(gateway.forwarded_by).to be_nil
    end

    it 'promotes a forwarded candidate via a direct GWINFO' do
      discovery.receive(gwinfo(4, gateway_address), relay_address)
      discovery.receive(gwinfo(6, gateway_address), gateway_address)

      gateway = discovery.select_gateway
      expect(gateway.gateway_id).to eq(6)
      expect(gateway).to be_direct
    end

    it 'never downgrades direct information with a relayed id claim' do
      discovery.receive(advertise(4, 20), gateway_address)
      clock.advance(5)
      discovery.receive(gwinfo(99, gateway_address), relay_address)

      expect(discovery.gateways.size).to eq(1)
      gateway = discovery.select_gateway
      expect(gateway.gateway_id).to eq(4)
      expect(gateway).to be_direct
    end

    it 'updates the id and reporter when relayed information is renewed' do
      discovery.receive(gwinfo(4, gateway_address), relay_address)
      discovery.receive(gwinfo(8, gateway_address), '10.4.0.4:5000')

      gateway = discovery.select_gateway
      expect(gateway.gateway_id).to eq(8)
      expect(gateway.forwarded_by).to eq('10.4.0.4:5000')
      expect(gateway).to_not be_direct
    end

    it 'prefers direct candidates over relayed candidates with equal validity' do
      discovery.receive(gwinfo(1, gateway_address), relay_address)
      discovery.receive(advertise(2, 20), other_address)

      expect(discovery.select_gateway.address).to eq(other_address)
    end

    it 'keeps same id, different addresses as separate candidates' do
      discovery.receive(advertise(3, 20), gateway_address)
      discovery.receive(gwinfo(3, other_address), relay_address)

      ids = discovery.gateways.map(&:gateway_id)
      addresses = discovery.gateways.map(&:address)
      expect(ids).to eq([3, 3])
      expect(addresses).to eq([gateway_address, other_address])
    end
  end

  describe 'expiry' do
    it 'removes candidates whose validity has elapsed during tick' do
      discovery.receive(advertise(7, 30), gateway_address)
      clock.advance(29)
      discovery.tick
      expect(discovery.gateways.size).to eq(1)

      clock.advance(1)
      expired = discovery.tick

      expect(discovery.gateways).to be_empty
      expect(expired).to eq([])
      expect(discovery.pruned_gateways.map(&:address)).to eq([gateway_address])
    end

    it 'returns nil from selection instead of declaring the session dead' do
      discovery.receive(advertise(7, 10), gateway_address)
      clock.advance(10)
      discovery.tick

      expect(discovery.select_gateway).to be_nil
    end

    it 'allows re-selection after the same gateway advertises again' do
      discovery.receive(advertise(7, 10), gateway_address)
      clock.advance(10)
      discovery.tick
      expect(discovery.select_gateway).to be_nil

      discovery.receive(advertise(7, 20), gateway_address)
      expect(discovery.select_gateway.address).to eq(gateway_address)
    end

    it 'expires GWINFO candidates using the configured ttl' do
      discovery.receive(gwinfo(4, gateway_address), relay_address)
      clock.advance(100)
      discovery.tick

      expect(discovery.gateways).to be_empty
    end
  end

  describe 'SEARCHGW handling' do
    it 'does not answer when no gateway is known' do
      discovery.receive(searchgw(1), relay_address)
      clock.advance(5)
      expect(discovery.tick).to eq([])
    end

    it 'answers the searcher with a delayed unicast GWINFO' do
      discovery.receive(advertise(7, 30), gateway_address)
      discovery.receive(searchgw(1), relay_address)

      expect(discovery.tick).to eq([])

      clock.advance(3)
      datagrams = discovery.tick
      expect(datagrams.size).to eq(1)

      payload, destination = datagrams.first
      expect(destination).to eq(relay_address)
      packet = MQTT::SN::Packet.parse(payload)
      expect(packet).to be_a(MQTT::SN::Packet::Gwinfo)
      expect(packet.gateway_id).to eq(7)
      expect(packet.gateway_address).to eq(gateway_address)
    end

    it 'answers repeated broadcasts only once inside the request window' do
      discovery.receive(advertise(7, 30), gateway_address)
      discovery.receive(searchgw(1), relay_address)
      discovery.receive(searchgw(1), relay_address)
      clock.advance(5)

      expect(discovery.tick.size).to eq(1)
    end

    it 'cancels the delayed answer once another client reports the gateway' do
      discovery.receive(advertise(7, 30), gateway_address)
      discovery.receive(searchgw(1), relay_address)
      discovery.receive(gwinfo(7, gateway_address), '10.9.9.9:5000')
      clock.advance(5)

      expect(discovery.tick).to eq([])
    end

    it 'does not answer once the gateway expires during the wait' do
      shortlived = MQTT::SN::Discovery.new(
        :clock => clock,
        :random => FixedRandom.new([4]),
        :response_delay => 1..5,
        :gateway_ttl => 3
      )
      shortlived.receive(gwinfo(7, gateway_address), gateway_address)
      shortlived.receive(searchgw(1), relay_address)
      clock.advance(4)
      expect(shortlived.tick).to eq([])
      expect(shortlived.gateways).to be_empty
    end

    it 'bounds the number of delayed replies kept pending' do
      bounded = MQTT::SN::Discovery.new(
        :clock => clock,
        :random => FixedRandom.new,
        :response_delay => 1..5,
        :max_pending_replies => 3
      )
      bounded.receive(advertise(7, 30), gateway_address)
      6.times { |n| bounded.receive(searchgw(1), "10.3.0.#{n}:5000") }
      clock.advance(5)

      expect(bounded.tick.size).to eq(3)
    end

    it 'queues only one reply for a searcher using different radii' do
      discovery.receive(advertise(7, 30), gateway_address)
      discovery.receive(searchgw(1), relay_address)
      discovery.receive(searchgw(2), relay_address)
      clock.advance(5)

      unicast = discovery.tick.select do |_payload, destination|
        destination == relay_address
      end
      expect(unicast.size).to eq(1)
    end

    it 'forwards radius greater than one with the radius reduced by one' do
      discovery.receive(advertise(7, 30), gateway_address)
      discovery.receive(searchgw(3), relay_address)

      clock.advance(1)
      datagrams = discovery.tick
      forwarded = datagrams.find do |payload, destination|
        destination == MQTT::SN::Discovery::BROADCAST_ADDRESS
      end
      expect(forwarded).to_not be_nil
      packet = MQTT::SN::Packet.parse(forwarded.first)
      expect(packet).to be_a(MQTT::SN::Packet::Searchgw)
      expect(packet.radius).to eq(2)
    end

    it 'never rebroadcasts a request that reached radius one' do
      discovery.receive(advertise(7, 30), gateway_address)
      discovery.receive(searchgw(1), relay_address)
      clock.advance(2)

      datagrams = discovery.tick
      broadcast = datagrams.any? do |_payload, destination|
        destination == MQTT::SN::Discovery::BROADCAST_ADDRESS
      end
      expect(broadcast).to be false
    end

    it 'limits how often requests are forwarded' do
      forwarder = MQTT::SN::Discovery.new(
        :clock => clock,
        :random => FixedRandom.new,
        :forward_delay => 1,
        :min_forward_interval => 10
      )

      broadcasts = lambda do
        forwarder.tick.select do |_payload, destination|
          destination == MQTT::SN::Discovery::BROADCAST_ADDRESS
        end
      end

      forwarder.receive(searchgw(2), '10.2.0.1:5000')
      forwarder.receive(searchgw(2), '10.2.0.2:5000')
      clock.advance(1)
      expect(broadcasts.call.size).to eq(1)

      clock.advance(8)
      forwarder.receive(searchgw(2), '10.2.0.3:5000')
      clock.advance(1)
      expect(broadcasts.call.size).to eq(0)

      clock.advance(2)
      forwarder.receive(searchgw(2), '10.2.0.4:5000')
      clock.advance(1)
      expect(broadcasts.call.size).to eq(1)
    end
  end

  describe 'candidate limits' do
    let(:discovery) do
      MQTT::SN::Discovery.new(
        :clock => clock,
        :random => FixedRandom.new,
        :max_candidates => 2,
        :gateway_ttl => 100
      )
    end

    it 'retains no more than the configured number of candidates' do
      discovery.receive(gwinfo(1, '10.0.0.1:1883'), '10.9.0.1:5000')
      discovery.receive(gwinfo(2, '10.0.0.2:1883'), '10.9.0.2:5000')
      discovery.receive(gwinfo(3, '10.0.0.3:1883'), '10.9.0.3:5000')

      expect(discovery.gateways.size).to eq(2)
    end

    it 'lets a better-ranked newcomer evict the worst candidate' do
      discovery.receive(gwinfo(1, '10.0.0.1:1883'), '10.9.0.1:5000')
      discovery.receive(gwinfo(2, '10.0.0.2:1883'), '10.9.0.2:5000')
      discovery.receive(advertise(3, 50), '10.0.0.3:1883')

      addresses = discovery.gateways.map(&:address)
      expect(addresses).to include('10.0.0.3:1883')
      expect(addresses.size).to eq(2)
    end
  end

  describe 'unrelated packets' do
    it 'ignores packet types that are not part of discovery' do
      ping = MQTT::SN::Packet::Pingresp.new.to_s
      expect(discovery.receive(ping, relay_address)).to eq([])
    end
  end

  describe 'datagram stream lifecycle' do
    it 'discovers, renews, expires and allows re-selection' do
      # [time, payload, source] entries, fed into the component in order
      datagrams = [
        [1001, gwinfo(7, gateway_address), relay_address],
        [1005, advertise(7, 20), gateway_address],
        [1006, searchgw(1), '10.3.0.1:5000'],
        [1007, searchgw(1), '10.3.0.1:5000'],
        [1030, advertise(7, 15), gateway_address]
      ]

      # The sensors do not know the gateway: originate a SEARCHGW broadcast
      expect(discovery.search!.last).to eq(MQTT::SN::Discovery::BROADCAST_ADDRESS)

      clock.advance(1)
      at, payload, source = datagrams.first
      discovery.receive(payload, source)
      first = discovery.select_gateway(at)
      expect(first.address).to eq(gateway_address)
      expect(first).to_not be_direct

      # Direct advertisement renews and promotes the same address
      clock.advance(4)
      at, payload, source = datagrams[1]
      discovery.receive(payload, source)
      promoted = discovery.select_gateway(at)
      expect(promoted).to be_direct
      expect(promoted.remaining(at)).to eq(20)

      # Repeated SEARCHGW produces one delayed reply, not amplification
      clock.advance(1)
      discovery.receive(datagrams[2][1], datagrams[2][2])
      discovery.receive(datagrams[3][1], datagrams[3][2])
      clock.advance(4)
      expect(discovery.tick.size).to eq(1)

      # Beyond validity: no choice, without declaring the session dead
      clock.advance(16)
      discovery.tick
      expect(discovery.select_gateway).to be_nil
      expect(discovery.pruned_gateways.map(&:address)).to eq([gateway_address])

      # A later advertisement makes the gateway selectable again
      clock.advance(4)
      at, payload, source = datagrams.last
      discovery.receive(payload, source)
      reselected = discovery.select_gateway
      expect(reselected.address).to eq(gateway_address)
      expect(reselected.remaining(clock.now)).to eq(15)
    end
  end
end

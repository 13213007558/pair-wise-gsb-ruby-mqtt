# encoding: BINARY

$:.unshift(File.dirname(__FILE__))

require 'spec_helper'

# Monotonic clock controlled by the tests, so discovery, renewal, expiry
# and re-selection can be checked without real time or real networking
class FakeClock
  def initialize
    @now = 0.0
  end

  attr_reader :now

  def advance(seconds)
    @now += seconds
  end

  def call
    @now
  end
end

describe MQTT::SN::GatewayDiscovery do
  let(:clock) { FakeClock.new }
  let(:discovery) { MQTT::SN::GatewayDiscovery.new(:clock => clock) }

  def advertise(gateway_id, duration)
    MQTT::SN::Packet::Advertise.new(:gateway_id => gateway_id, :duration => duration).to_s
  end

  def gwinfo(gateway_id, address = nil)
    MQTT::SN::Packet::Gwinfo.new(:gateway_id => gateway_id, :gateway_address => address).to_s
  end

  describe 'learning gateways from ADVERTISE broadcasts' do
    it 'adds a candidate addressed by the datagram source with the advertised validity' do
      result = discovery.receive_datagram(advertise(7, 30), 'udp://10.0.0.1:1883')

      expect(result).to eq(:added)
      candidate = discovery.best_gateway
      expect(candidate.gateway_id).to eq(7)
      expect(candidate.address).to eq('udp://10.0.0.1:1883')
      expect(candidate.source).to eq(:gateway)
      expect(candidate.ttl(clock.now)).to eq(30)
    end

    it 'ignores an ADVERTISE with zero duration' do
      expect(discovery.receive_datagram(advertise(7, 0), 'udp://10.0.0.1:1883')).to eq(:ignored)
      expect(discovery.candidates).to be_empty
    end

    it 'renews a gateway that advertises again from the same address' do
      discovery.receive_datagram(advertise(7, 30), 'udp://10.0.0.1:1883')
      clock.advance(20)

      result = discovery.receive_datagram(advertise(7, 30), 'udp://10.0.0.1:1883')

      expect(result).to eq(:renewed)
      expect(discovery.candidates.length).to eq(1)
      expect(discovery.best_gateway.ttl(clock.now)).to eq(30)
      expect(discovery.conflicts).to be_empty
    end

    it 'updates the validity when the same gateway advertises a new duration' do
      discovery.receive_datagram(advertise(7, 30), 'udp://10.0.0.1:1883')
      clock.advance(10)
      discovery.receive_datagram(advertise(7, 100), 'udp://10.0.0.1:1883')

      expect(discovery.best_gateway.ttl(clock.now)).to eq(100)
    end
  end

  describe 'expiry and re-selection' do
    it 'drops candidates once their validity has elapsed' do
      discovery.receive_datagram(advertise(7, 30), 'udp://10.0.0.1:1883')
      clock.advance(31)

      expect(discovery.candidates).to be_empty
      expect(discovery.best_gateway).to be_nil
    end

    it 'returns expired candidates from prune without touching anything else' do
      discovery.receive_datagram(advertise(7, 30), 'udp://10.0.0.1:1883')
      clock.advance(31)

      expired = discovery.prune

      expect(expired.map(&:gateway_id)).to eq([7])
      # Expiry only empties the suggestion list; the component itself keeps
      # working and can discover a replacement gateway afterwards
      expect(discovery.receive_datagram(advertise(8, 30), 'udp://10.0.0.2:1883')).to eq(:added)
      expect(discovery.best_gateway.gateway_id).to eq(8)
    end

    it 're-selects the remaining gateway when the preferred one expires' do
      discovery.receive_datagram(advertise(7, 30), 'udp://10.0.0.1:1883')
      clock.advance(1)
      discovery.receive_datagram(advertise(8, 100), 'udp://10.0.0.2:1883')

      expect(discovery.best_gateway.gateway_id).to eq(8)
      clock.advance(30)

      expect(discovery.best_gateway.gateway_id).to eq(8)
      clock.advance(71)

      expect(discovery.best_gateway).to be_nil
    end
  end

  describe 'sending SEARCHGW' do
    it 'returns an encoded SEARCHGW packet with the requested radius' do
      bytes = discovery.search(2)

      packet = MQTT::SN::Packet.parse(bytes)
      expect(packet).to be_a(MQTT::SN::Packet::Searchgw)
      expect(packet.radius).to eq(2)
    end

    it 'clamps the radius to the configured maximum' do
      packet = MQTT::SN::Packet.parse(discovery.search(99))
      expect(packet.radius).to eq(MQTT::SN::GatewayDiscovery::DEFAULT_MAX_RADIUS)
    end

    it 'rate limits repeated searches' do
      expect(discovery.search).not_to be_nil
      expect(discovery.search).to be_nil

      clock.advance(MQTT::SN::GatewayDiscovery::DEFAULT_MIN_SEARCH_INTERVAL)
      expect(discovery.search).not_to be_nil
    end

    it 'is not re-armed by incoming broadcasts, so they cannot amplify requests' do
      expect(discovery.search).not_to be_nil
      20.times { discovery.receive_datagram(advertise(7, 30), 'udp://10.0.0.1:1883') }

      expect(discovery.search).to be_nil
    end
  end

  describe 'handling GWINFO replies' do
    it 'treats a GWINFO without an address as coming from the gateway itself' do
      discovery.search
      clock.advance(0.5)

      result = discovery.receive_datagram(gwinfo(3), 'udp://10.0.0.3:1883')

      expect(result).to eq(:added)
      candidate = discovery.best_gateway
      expect(candidate.source).to eq(:gateway)
      expect(candidate.address).to eq('udp://10.0.0.3:1883')
    end

    it 'treats a GWINFO naming a different address as client-forwarded information' do
      discovery.search
      clock.advance(0.5)

      result = discovery.receive_datagram(gwinfo(3, 'udp://10.0.0.3:1883'), 'udp://10.0.0.99:1883')

      expect(result).to eq(:added)
      candidate = discovery.best_gateway
      expect(candidate.source).to eq(:client)
      expect(candidate.address).to eq('udp://10.0.0.3:1883')
    end

    it 'drops a GWINFO that arrives after the response window' do
      discovery.search
      clock.advance(MQTT::SN::GatewayDiscovery::DEFAULT_RESPONSE_WINDOW + 0.1)

      expect(discovery.receive_datagram(gwinfo(3), 'udp://10.0.0.3:1883')).to eq(:ignored_late)
      expect(discovery.candidates).to be_empty
    end

    it 'drops a GWINFO when no search is outstanding' do
      expect(discovery.receive_datagram(gwinfo(3), 'udp://10.0.0.3:1883')).to eq(:ignored_late)
    end
  end

  describe 'gateway id conflicts' do
    it 'lets the newest gateway-confirmed address win and records the conflict' do
      discovery.receive_datagram(advertise(7, 30), 'udp://10.0.0.1:1883')
      clock.advance(1)

      result = discovery.receive_datagram(advertise(7, 30), 'udp://10.0.0.2:1883')

      expect(result).to eq(:conflict)
      expect(discovery.candidates.length).to eq(1)
      expect(discovery.best_gateway.address).to eq('udp://10.0.0.2:1883')
      expect(discovery.conflicts.length).to eq(1)
      expect(discovery.conflicts.first[:gateway_id]).to eq(7)
      expect(discovery.conflicts.first[:kept_address]).to eq('udp://10.0.0.2:1883')
      expect(discovery.conflicts.first[:dropped_address]).to eq('udp://10.0.0.1:1883')
    end

    it 'keeps gateway-confirmed information over client-forwarded information' do
      discovery.receive_datagram(advertise(7, 30), 'udp://10.0.0.1:1883')
      discovery.search

      result = discovery.receive_datagram(gwinfo(7, 'udp://10.0.0.2:1883'), 'udp://10.0.0.99:1883')

      expect(result).to eq(:conflict)
      expect(discovery.best_gateway.address).to eq('udp://10.0.0.1:1883')
      expect(discovery.conflicts.first[:kept_address]).to eq('udp://10.0.0.1:1883')
    end

    it 'upgrades a client-forwarded candidate when the gateway confirms the same address' do
      discovery.search
      discovery.receive_datagram(gwinfo(7, 'udp://10.0.0.1:1883'), 'udp://10.0.0.99:1883')
      clock.advance(1)

      result = discovery.receive_datagram(advertise(7, 30), 'udp://10.0.0.1:1883')

      expect(result).to eq(:renewed)
      expect(discovery.best_gateway.source).to eq(:gateway)
      expect(discovery.conflicts).to be_empty
    end
  end

  describe 'candidate ordering' do
    it 'prefers gateway-confirmed candidates over client-forwarded ones' do
      discovery.search
      discovery.receive_datagram(gwinfo(1, 'udp://10.0.0.1:1883'), 'udp://10.0.0.99:1883')
      discovery.receive_datagram(advertise(2, 30), 'udp://10.0.0.2:1883')

      expect(discovery.best_gateway.gateway_id).to eq(2)
      expect(discovery.candidates.map(&:source)).to eq([:gateway, :client])
    end
  end

  describe 'bounding the candidate list' do
    let(:discovery) { MQTT::SN::GatewayDiscovery.new(:clock => clock, :max_candidates => 2) }

    it 'evicts the candidate with the earliest expiry when full' do
      discovery.receive_datagram(advertise(1, 10), 'udp://10.0.0.1:1883')
      discovery.receive_datagram(advertise(2, 100), 'udp://10.0.0.2:1883')

      expect(discovery.receive_datagram(advertise(3, 50), 'udp://10.0.0.3:1883')).to eq(:added)

      expect(discovery.candidates.map(&:gateway_id).sort).to eq([2, 3])
    end

    it 'drops client-forwarded information when the list is full of gateways' do
      discovery.receive_datagram(advertise(1, 100), 'udp://10.0.0.1:1883')
      discovery.receive_datagram(advertise(2, 100), 'udp://10.0.0.2:1883')
      discovery.search

      result = discovery.receive_datagram(gwinfo(3, 'udp://10.0.0.3:1883'), 'udp://10.0.0.99:1883')

      expect(result).to eq(:dropped)
      expect(discovery.candidates.map(&:gateway_id).sort).to eq([1, 2])
    end

    it 'makes room by evicting expired candidates first' do
      discovery.receive_datagram(advertise(1, 10), 'udp://10.0.0.1:1883')
      discovery.receive_datagram(advertise(2, 100), 'udp://10.0.0.2:1883')
      clock.advance(11)

      expect(discovery.receive_datagram(advertise(3, 50), 'udp://10.0.0.3:1883')).to eq(:added)
      expect(discovery.candidates.map(&:gateway_id).sort).to eq([2, 3])
    end
  end

  describe 'robustness' do
    it 'ignores malformed datagrams' do
      expect(discovery.receive_datagram("ÿgarbage".b, 'udp://10.0.0.1:1883')).to eq(:ignored)
      expect(discovery.candidates).to be_empty
    end

    it 'ignores datagrams that are not discovery related' do
      pingreq = MQTT::SN::Packet::Pingreq.new.to_s
      expect(discovery.receive_datagram(pingreq, 'udp://10.0.0.1:1883')).to eq(:ignored)
    end
  end
end

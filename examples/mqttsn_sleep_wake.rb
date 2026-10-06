#!/usr/bin/env ruby
#
# MQTT-SN sleep / wake demonstration without a real gateway.
#
# A ScriptedTransport stands in for the datagram network: the "gateway" is
# a thread that inspects every packet the client sends and scripts the
# datagrams the client should receive. The clock is a fake clock that
# advances one simulated second every 0.01 real seconds while the client is
# blocked in the transport, so keep-alive timing is fully deterministic.
#

$:.unshift File.dirname(__FILE__) + '/../lib'

require 'thread'
require 'timeout'
require 'mqtt'

# A controllable monotonic clock. While the client waits inside the
# transport, #tick advances simulated time, so keep-alive deadlines fire
# instantly instead of waiting real seconds.
class ScriptClock
  def initialize
    @now = 0.0
    @mutex = Mutex.new
    @cond = ConditionVariable.new
    @ticking = false
  end

  def now
    @mutex.synchronize { @now }
  end

  def tick
    @mutex.synchronize do
      @now += 1
      @cond.broadcast
    end
  end

  # Wait up to +seconds+ of *simulated* time.
  def wait_simulated(seconds)
    deadline = now + seconds
    @mutex.synchronize do
      @cond.wait(@mutex, 0.05) while now < deadline
    end
  end
end

# Datagram transport driven entirely by a script. The +director+ block is
# run in a thread and decides what to enqueue in response to each packet
# the client sends.
class ScriptedTransport
  attr_reader :sent

  def initialize(clock, &director)
    @clock = clock
    @director_block = director
    @inbox = Queue.new
    @sent = Queue.new
    @closed = false
    @director = Thread.new do
      Thread.current.abort_on_exception = true
      loop do
        packet = @sent.pop
        break if packet == :stop

        instance_exec(MQTT::SN::Packet.parse(packet), &@director_block)
      end
    end
  end

  def send(data)
    raise IOError, 'transport closed' if @closed

    @sent << data
  end

  def recv(timeout = nil)
    if timeout.nil?
      item = @inbox.pop
    else
      # Wake every 0.01 real seconds to advance the simulated clock; the
      # client itself decides when its simulated deadline has passed.
      item = Timeout.timeout(0.01) { @inbox.pop } rescue nil
      @clock.tick if item.nil?
    end
    item == :close ? nil : item
  end

  def deliver(data)
    @inbox << data
  end

  def close
    @closed = true
    @sent << :stop
  end

  def join
    @director.join
  end
end

def banner(title)
  puts
  puts "== #{title} " + '=' * (68 - title.length)
end

# ---------------------------------------------------------------------------
# Scenario 1: a full sleep/wake round
# ---------------------------------------------------------------------------
banner('1. connect, on-line heartbeat, sleep, drain backlog, sleep again')

clock = ScriptClock.new
transport = ScriptedTransport.new(clock) do |packet|
  case packet
  when MQTT::SN::Packet::Connect
    deliver(MQTT::SN::Packet::Connack.new(:return_code => 0).to_s)
  when MQTT::SN::Packet::Pingreq
    if packet.client_id
      # A waking client: replay two buffered messages, then end the wake-up.
      deliver(MQTT::SN::Packet::Publish.new(
                :topic_id => 'T1', :topic_id_type => :short,
                :id => 0, :qos => 0, :data => 'buffered-qos0').to_s)
      deliver(MQTT::SN::Packet::Publish.new(
                :topic_id => 'T2', :topic_id_type => :short,
                :id => 44, :qos => 1, :data => 'buffered-qos1').to_s)
      deliver(MQTT::SN::Packet::Pingresp.new.to_s)
    else
      # Ordinary on-line keep-alive heartbeat.
      deliver(MQTT::SN::Packet::Pingresp.new.to_s)
    end
  when MQTT::SN::Packet::Puback
    # PUBACK sent by the client to acknowledge buffered QoS 1 mail
  when MQTT::SN::Packet::Disconnect
    if packet.duration
      # Grant exactly the sleep duration that was requested.
      deliver(MQTT::SN::Packet::Disconnect.new(:duration => packet.duration).to_s)
    end
  end
end

client = MQTT::SN::Client.new(transport,
                              :client_id => 'sensor-1',
                              :keep_alive => 15,
                              :ack_timeout => 5,
                              :clock => clock)
client.connect
puts "connected; state=#{client.state}"

# Block briefly: the fake clock crosses the 15s keep-alive boundary and an
# ordinary (client-id-less) PINGREQ / PINGRESP heartbeat happens.
puts 'waiting 20 simulated seconds on-line ...'
client.get(20)
puts "heartbeat done; state stays #{client.state}"

granted = client.sleep(300)
puts "asleep for #{granted}s; state=#{client.state}, #{client.sleep_remaining.round}s left"

# While asleep no ordinary keep-alive happens: a short get just times out and
# the remaining sleep time is untouched (the fake clock does not advance, the
# client is not pumping the transport).
puts "sleeping: get_packet returns #{client.get_packet(0.01).inspect}"

drained = client.wake do |message|
  puts "  backlog while waking: #{message.topic_id} -> #{message.data} (qos #{message.qos})"
end
puts "woke up; drained #{drained.length} buffered message(s); state=#{client.state}"

# The PINGRESP ended the wake-up: any later PINGRESP is a stray heartbeat
# reply and must not restart a drain.
client.sleep(600)
puts "sleeping again; state=#{client.state}"
client.disconnect
transport.join
puts "client ended the session; state=#{client.state}"

# ---------------------------------------------------------------------------
# Scenario 2: acknowledgement timeout is observable
# ---------------------------------------------------------------------------
banner('2. timeout: gateway never answers a QoS 1 PUBLISH')

clock2 = ScriptClock.new
transport2 = ScriptedTransport.new(clock2) do |packet|
  case packet
  when MQTT::SN::Packet::Connect
    deliver(MQTT::SN::Packet::Connack.new(:return_code => 0).to_s)
  when MQTT::SN::Packet::Disconnect
    deliver(MQTT::SN::Packet::Disconnect.new(:duration => packet.duration).to_s) if packet.duration
  end
end

client2 = MQTT::SN::Client.new(transport2,
                              :client_id => 'sensor-2',
                              :keep_alive => 100,
                              :ack_timeout => 5,
                              :clock => clock2)
client2.connect
begin
  client2.publish('TT', 'lost in the network', false, 1)
rescue MQTT::SN::TimeoutException => e
  puts "observed timeout: #{e.message}; state=#{client2.state}"
end
transport2.close
transport2.join

# ---------------------------------------------------------------------------
# Scenario 3: gateway lost the session while the client was sleeping
# ---------------------------------------------------------------------------
banner('3. session lost: gateway answers the wake-up with DISCONNECT')

clock3 = ScriptClock.new
transport3 = ScriptedTransport.new(clock3) do |packet|
  case packet
  when MQTT::SN::Packet::Connect
    deliver(MQTT::SN::Packet::Connack.new(:return_code => 0).to_s)
  when MQTT::SN::Packet::Disconnect
    deliver(MQTT::SN::Packet::Disconnect.new(:duration => packet.duration).to_s) if packet.duration
  when MQTT::SN::Packet::Pingreq
    deliver(MQTT::SN::Packet::Disconnect.new.to_s) # duration-less: session gone
  end
end

client3 = MQTT::SN::Client.new(transport3,
                              :client_id => 'sensor-3',
                              :keep_alive => 100,
                              :ack_timeout => 5,
                              :clock => clock3)
client3.connect
client3.sleep(300)
begin
  client3.wake
rescue MQTT::SN::SessionLostException => e
  puts "observed lost session: #{e.message}; state=#{client3.state}"
end
transport3.join

# ---------------------------------------------------------------------------
# Scenario 4: client actively ends the session with a plain DISCONNECT
# ---------------------------------------------------------------------------
banner('4. client initiated disconnect')

clock4 = ScriptClock.new
captured = []
transport4 = ScriptedTransport.new(clock4) do |packet|
  case packet
  when MQTT::SN::Packet::Connect
    deliver(MQTT::SN::Packet::Connack.new(:return_code => 0).to_s)
  when MQTT::SN::Packet::Disconnect
    captured << packet
  end
end

MQTT::SN::Client.connect(transport4,
                         :client_id => 'sensor-4',
                         :keep_alive => 100,
                         :clock => clock4) do |session|
  session.publish('TT', 'goodbye')
end
transport4.join
final = captured.last
puts "final packet: DISCONNECT duration=#{final.duration.inspect}; transport closed=#{transport4.instance_variable_get(:@closed)}"

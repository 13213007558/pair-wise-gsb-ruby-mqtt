#!/usr/bin/env ruby
#
# Demonstrates a full MQTT-SN sensor session without a real gateway:
#
#   connect -> subscribe -> publish (QoS 0 and 1) -> sleep ->
#   gateway buffers messages -> wake and drain the backlog ->
#   sleep again -> client-initiated disconnect
#
# The "gateway" is a scripted in-process object and the clock is a
# fake, so the whole run is deterministic and needs no network.

$:.unshift File.dirname(__FILE__)+'/../lib'

require 'mqtt'

# A clock the script controls; time only passes when we say so
class DemoClock
  attr_reader :now

  def initialize
    @now = 0.0
  end

  def sleep(seconds)
    @now += seconds
  end

  def advance(seconds)
    @now += seconds
  end
end

# Plays the role of a fixed MQTT-SN gateway. It is also the datagram
# transport handed to the session: +send+ delivers a packet to the
# gateway, +recv+ hands the next gateway datagram to the client.
class ScriptedGateway
  TOPIC_ID = 0x0001

  def initialize(clock)
    @clock = clock
    @to_client = []
    @buffered = []
    @asleep = false
    @next_id = 100
  end

  # ---- transport interface used by MQTT::SN::Session ----

  def send(bytes)
    packet = MQTT::SN::Packet.parse(bytes)
    puts "  client -> gateway: #{describe(packet)}"
    handle(packet)
  end

  def recv
    @to_client.shift
  end

  # ---- scripted gateway behaviour ----

  def handle(packet)
    case packet
    when MQTT::SN::Packet::Connect
      reply MQTT::SN::Packet::Connack.new(:return_code => 0x00)
    when MQTT::SN::Packet::Subscribe
      reply MQTT::SN::Packet::Suback.new(:id => packet.id, :topic_id => TOPIC_ID, :return_code => 0x00)
    when MQTT::SN::Packet::Publish
      if packet.qos == 1
        reply MQTT::SN::Packet::Puback.new(:topic_id => packet.topic_id, :id => packet.id)
      end
    when MQTT::SN::Packet::Pingreq
      if packet.client_id
        # A waking client: flush everything we buffered, then PINGRESP
        @asleep = false
        @buffered.each { |message| reply message }
        @buffered.clear
        reply MQTT::SN::Packet::Pingresp.new
      else
        reply MQTT::SN::Packet::Pingresp.new
      end
    when MQTT::SN::Packet::Disconnect
      @asleep = !packet.duration.nil?
      reply MQTT::SN::Packet::Disconnect.new
    end
  end

  # The outside world publishes to the client's topic; the gateway
  # delivers immediately or buffers while the client sleeps
  def sensor_reading(data)
    message = MQTT::SN::Packet::Publish.new(
      :topic_id => TOPIC_ID, :id => @next_id, :data => data, :qos => 1
    )
    @next_id += 1
    if @asleep
      puts "  gateway buffers:   #{describe(message)}"
      @buffered << message
    else
      reply message
    end
  end

  def reply(packet)
    puts "  gateway -> client: #{describe(packet)}"
    @to_client << packet.to_s
  end

  def describe(packet)
    name = packet.class.name.split('::').last.upcase
    case packet
    when MQTT::SN::Packet::Connect
      "#{name}(client_id=#{packet.client_id}, keep_alive=#{packet.keep_alive})"
    when MQTT::SN::Packet::Disconnect
      packet.duration ? "#{name}(duration=#{packet.duration})" : name
    when MQTT::SN::Packet::Pingreq
      packet.client_id ? "#{name}(client_id=#{packet.client_id})" : name
    when MQTT::SN::Packet::Publish
      "#{name}(topic_id=#{packet.topic_id}, id=#{packet.id}, qos=#{packet.qos}, data=#{packet.data.inspect})"
    when MQTT::SN::Packet::Subscribe
      "#{name}(topic=#{packet.topic_name}, id=#{packet.id})"
    else
      name
    end
  end
end


clock = DemoClock.new
gateway = ScriptedGateway.new(clock)

session = MQTT::SN::Session.new(
  :transport => gateway,
  :clock => clock,
  :client_id => 'field-sensor-7',
  :keep_alive => 60,
  :ack_timeout => 10
)
session.on_event = lambda { |event, *details| puts ">> event: #{[event, *details].inspect}" }
session.on_message = lambda { |packet| puts ">> message received: #{packet.data.inspect} (topic_id=#{packet.topic_id})" }

puts '== connect =='
session.connect
topic_id = session.subscribe('field/sensor-7/reading', :qos => 1)

puts '== publish a reading (QoS 1) and a heartbeat (QoS 0) =='
session.publish(topic_id, '22.5', :qos => 1)
session.publish(topic_id, 'ok', :qos => 0)

puts '== idle past the keep-alive interval =='
clock.advance(61)
session.tick

puts '== sleep for 30 minutes =='
session.sleep(1800)

puts '== readings arrive while the device sleeps =='
gateway.sensor_reading('22.9')
gateway.sensor_reading('23.1')

puts '== wake up and drain the backlog =='
session.wake

puts '== sleep again, then shut down for good =='
session.sleep(1800)
session.disconnect

puts "== final state: #{session.state} =="

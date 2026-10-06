# encoding: BINARY

$:.unshift(File.dirname(__FILE__))

require 'spec_helper'
require 'mqtt'

describe MQTT::InMemorySessionStore do
  let(:store) { described_class.new }

  it 'returns nil for an unknown session' do
    expect(store.load_session('client')).to be_nil
  end

  it 'returns a copy of saved session data' do
    session = { 'outgoing_packet_id' => 1, 'outgoing_publishes' => {} }
    store.save_session('client', session)
    session['outgoing_packet_id'] = 2

    expect(store.load_session('client')['outgoing_packet_id']).to eq(1)
  end

  it 'does not return a mutable copy of saved session data' do
    store.save_session('client', { 'outgoing_packet_id' => 1 })
    store.load_session('client')['outgoing_packet_id'] = 2

    expect(store.load_session('client')['outgoing_packet_id']).to eq(1)
  end

  it 'deletes saved session data' do
    store.save_session('client', { 'outgoing_packet_id' => 1 })
    store.delete_session('client')

    expect(store.load_session('client')).to be_nil
  end
end

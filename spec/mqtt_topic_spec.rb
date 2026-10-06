# encoding: BINARY

require 'spec_helper'
require 'mqtt'

describe MQTT::Topic do
  describe '.split_shared' do
    it 'returns the group and filter for a shared subscription' do
      expect(described_class.split_shared('$share/group1/a/b'))
        .to eq(['group1', 'a/b'])
    end

    it 'returns nil group for an ordinary filter' do
      expect(described_class.split_shared('a/b')).to eq([nil, 'a/b'])
    end

    it 'uses the first level after $share/ as the group name' do
      expect(described_class.split_shared('$share/a/b')).to eq(['a', 'b'])
    end
  end

  describe '.match?' do
    {
      'a/b' => [['a/b', true], ['a/c', false], ['a', false], ['a/b/c', false]],
      'a/+' => [['a/b', true], ['a/c', true], ['b/c', false], ['a/b/c', false]],
      'a/#' => [['a/b', true], ['a/b/c', true], ['a', false], ['b/c', false]],
      '#' => [['a/b/c', true], ['a', true]],
      '+/b' => [['a/b', true], ['c/b', true], ['a/c', false]],
      'sport/tennis/player1' => [['sport/tennis/player1', true], ['sport/tennis/player2', false]],
      '$SYS/#' => [['$SYS/status', true], ['status', false]],
      '+' => [['$SYS', false], ['a', true]],
      '$share/group1/a/+' => [['a/b', true], ['a/b/c', false], ['group1/a/b', false]],
      '$share/g/a/#' => [['a/b/c', true], ['g/a', false]]
    }.each do |filter, cases|
      cases.each do |topic, result|
        it "returns #{result} for filter #{filter.inspect} against topic #{topic.inspect}" do
          expect(described_class.match?(filter, topic)).to eq(result)
        end
      end
    end

    it 'does not use the shared subscription group name for matching' do
      expect(described_class.match?('$share/machines/sensor/+/temp', 'sensor/42/temp')).to eq(true)
      expect(described_class.match?('$share/machines/sensor/+/temp', 'machines/sensor/42/temp')).to eq(false)
    end
  end
end

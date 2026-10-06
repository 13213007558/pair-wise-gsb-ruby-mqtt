# frozen_string_literal: true

$:.unshift(File.dirname(__FILE__))

require 'spec_helper'
require 'mqtt'

describe MQTT::TopicFilter do
  describe '.match?' do
    it 'matches an exact topic name' do
      expect(described_class.match?('a/b/c', 'a/b/c')).to be_truthy
    end

    it 'does not match a different exact topic name' do
      expect(described_class.match?('a/b/c', 'a/b/d')).to be_falsey
      expect(described_class.match?('a/b', 'a/b/c')).to be_falsey
    end

    it 'matches a single level wildcard' do
      expect(described_class.match?('a/+/c', 'a/b/c')).to be_truthy
      expect(described_class.match?('a/+/c', 'a/x/c')).to be_truthy
      expect(described_class.match?('+', 'a')).to be_truthy
    end

    it 'does not match multiple levels with a single level wildcard' do
      expect(described_class.match?('a/+', 'a/b/c')).to be_falsey
      expect(described_class.match?('a/+/c', 'a/b')).to be_falsey
    end

    it 'matches a multi level wildcard' do
      expect(described_class.match?('a/#', 'a/b/c')).to be_truthy
      expect(described_class.match?('a/#', 'a/b')).to be_truthy
      expect(described_class.match?('a/#', 'a')).to be_truthy
      expect(described_class.match?('#', 'anything/at/all')).to be_truthy
    end

    it 'does not match outside the prefixed level with a multi level wildcard' do
      expect(described_class.match?('a/#', 'b/c')).to be_falsey
    end

    it 'never matches $ topics with leading wildcards' do
      expect(described_class.match?('#', '$SYS/foo')).to be_falsey
      expect(described_class.match?('+', '$SYS')).to be_falsey
      expect(described_class.match?('+/foo', '$SYS/foo')).to be_falsey
    end

    it 'matches $ topics explicitly' do
      expect(described_class.match?('$SYS/foo', '$SYS/foo')).to be_truthy
      expect(described_class.match?('$SYS/+', '$SYS/foo')).to be_truthy
    end

    it 'rejects malformed wildcard usage' do
      expect(described_class.match?('a/b#', 'a/bc')).to be_falsey
      expect(described_class.match?('a/+x', 'a/bx')).to be_falsey
      expect(described_class.match?('a/#/c', 'a/b/c')).to be_falsey
    end

    context 'with shared subscription filters' do
      it 'splits the group name and actual filter' do
        expect(described_class.split_shared('$share/group/a/b')).to eq(['group', 'a/b'])
        expect(described_class.split_shared('a/b')).to be_nil
      end

      it 'matches using only the actual filter and ignores the group name' do
        expect(described_class.match?('$share/group/a/+', 'a/b')).to be_truthy
        expect(described_class.match?('$share/group/a/+', 'c/b')).to be_falsey
        expect(described_class.match?('$share/other/a/#', 'a/b/c')).to be_truthy
      end

      it 'does not treat the group name as a topic level' do
        expect(described_class.match?('$share/group/a', 'group')).to be_falsey
        expect(described_class.match?('$share/group/a', 'a')).to be_truthy
      end

      it 'returns false for malformed shared subscription filters' do
        expect(described_class.split_shared('$share/group')).to be_nil
        expect(described_class.split_shared('$share//a/b')).to be_nil
      end
    end
  end
end

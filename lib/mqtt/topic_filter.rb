# frozen_string_literal: true

module MQTT
  # Utility methods for matching MQTT topic names against topic filters.
  #
  # Supports the MQTT 5 shared subscription filter syntax:
  #   $share/<group-name>/<actual-filter>
  #
  # The shared subscription group name never takes part in topic matching;
  # only the filter following the group name is matched against.
  module TopicFilter
    module_function

    MULTI_LEVEL_WILDCARD = '#'.freeze
    SINGLE_LEVEL_WILDCARD = '+'.freeze
    SHARED_SUBSCRIPTION_PREFIX = '$share/'.freeze

    # Check whether a topic name matches a topic filter.
    # Works for both normal filters and shared subscription filters.
    def match?(filter, topic)
      return false if filter.nil? || topic.nil?
      _, actual_filter = split_shared(filter)
      actual_filter = filter if actual_filter.nil?
      levels = actual_filter.split('/')
      return false unless valid_filter_levels?(levels)
      levels_match(levels, topic.split('/'))
    end

    # Split a filter into [group_name, actual_filter].
    # Returns nil when the filter is not a shared subscription filter.
    def split_shared(filter)
      return nil unless shared?(filter)

      parts = filter.split('/')
      return nil if parts.length < 3
      return nil if parts[1].empty?

      actual_filter = parts[2..].join('/')
      return nil if actual_filter.empty?

      [parts[1], actual_filter]
    end

    # Returns true when the given filter uses the shared subscription syntax
    def shared?(filter)
      filter.to_s.start_with?(SHARED_SUBSCRIPTION_PREFIX)
    end

    def valid_filter_levels?(levels)
      levels.each_with_index do |level, index|
        next unless level.include?(MULTI_LEVEL_WILDCARD)
        return false unless level == MULTI_LEVEL_WILDCARD
        return false unless index == levels.length - 1
      end
      levels.each do |level|
        return false if level.include?(SINGLE_LEVEL_WILDCARD) && level != SINGLE_LEVEL_WILDCARD
      end
      true
    end

    def levels_match(filter_levels, topic_levels)
      filter_levels.each_with_index do |filter_level, index|
        topic_level = topic_levels[index]

        if filter_level == MULTI_LEVEL_WILDCARD
          # '#' must be the final filter level and matches zero or more
          # remaining topic levels, but never a topic level starting with '$'
          # when it is the first level.
          return index == filter_levels.length - 1 unless index.zero? && topic_level.to_s.start_with?('$')
          return false
        end

        # The topic must have a level to match against
        return false if topic_level.nil?

        # Wildcards on the first level must not match a leading '$'
        if index.zero? && topic_level.start_with?('$')
          return false if filter_level == SINGLE_LEVEL_WILDCARD
        end

        return false unless filter_level == SINGLE_LEVEL_WILDCARD || filter_level == topic_level
      end

      filter_levels.length == topic_levels.length
    end
  end
end

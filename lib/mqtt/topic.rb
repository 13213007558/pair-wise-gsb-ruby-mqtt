# Helpers for matching MQTT topic filters against topic names
module MQTT
  module Topic
    # Prefix of MQTT 5 shared subscription filters
    SHARED_PREFIX = '$share/'

    # Split a shared subscription filter into its group name and the actual filter.
    # Returns nil as the group name for non-shared filters.
    #
    # Example:
    #   MQTT::Topic.split_shared('$share/group/a/b')  #=> ['group', 'a/b']
    #   MQTT::Topic.split_shared('a/b')              #=> [nil, 'a/b']
    #
    def self.split_shared(filter)
      if filter.start_with?(SHARED_PREFIX)
        remainder = filter[SHARED_PREFIX.length..-1]
        separator = remainder.index('/')
        if separator.nil? || separator.zero?
          [nil, filter]
        else
          [remainder[0...separator], remainder[(separator + 1)..-1]]
        end
      else
        [nil, filter]
      end
    end

    # Returns true if the topic filter matches the topic name.
    #
    # Supports single level (+) and multi level (#) wildcard filters.
    # The group name of a shared subscription ($share/group/...) is not part
    # of the filter used for matching.
    def self.match?(filter, topic)
      _group, filter = split_shared(filter)
      filter_levels = filter.split('/')
      topic_levels = topic.split('/')

      filter_levels.each_with_index do |filter_level, index|
        topic_level = topic_levels[index]

        # No more topic levels: # needs a level of its own, nothing else matches
        return false if topic_level.nil?

        # A leading dollar sign topic is never matched by a wildcard
        if topic_level.start_with?('$') && index.zero? && filter_level != topic_level
          return false
        end

        # A multi-level wildcard matches this and all remaining levels
        return true if filter_level == '#'

        next if filter_level == '+' || filter_level == topic_level

        return false
      end

      filter_levels.length == topic_levels.length
    end
  end
end

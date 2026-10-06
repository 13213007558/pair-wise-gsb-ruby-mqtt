# encoding: BINARY

module MQTT
  # Class representing a MQTT Packet
  # Performs binary encoding and decoding of headers
  class Packet
    # The version number of the MQTT protocol to use (default 3.1.0)
    attr_accessor :version

    # Identifier to link related control packets together
    attr_accessor :id

    # Array of 4 bits in the fixed header
    attr_accessor :flags

    # The length of the parsed packet body
    attr_reader :body_length

    # MQTT 5 properties (a Hash of property name to value)
    attr_accessor :properties

    # Default attribute values
    ATTR_DEFAULTS = {
      :version => '3.1.0',
      :id => 0,
      :body_length => nil,
      :properties => {}
    }

    # Read in a packet from a socket
    def self.read(socket, version = nil)
      # Read in the packet header and create a new packet object
      packet = create_from_header(
        read_byte(socket),
        version
      )
      packet.validate_flags

      # Read in the packet length
      multiplier = 1
      body_length = 0
      pos = 1

      loop do
        digit = read_byte(socket)
        body_length += ((digit & 0x7F) * multiplier)
        multiplier *= 0x80
        pos += 1
        break if (digit & 0x80).zero? || pos > 4
      end

      # Store the expected body length in the packet
      packet.instance_variable_set('@body_length', body_length)

      # Read in the packet body
      packet.parse_body(socket.read(body_length))

      packet
    end

    # Parse buffer into new packet object
    def self.parse(buffer, version = nil)
      packet = parse_header(buffer, version)
      packet.parse_body(buffer)
      packet
    end

    # Parse the header and create a new packet object of the correct type
    # The header is removed from the buffer passed into this function
    def self.parse_header(buffer, version = nil)
      # Check that the packet is a long as the minimum packet size
      if buffer.bytesize < 2
        raise ProtocolException, 'Invalid packet: less than 2 bytes long'
      end

      # Create a new packet object
      bytes = buffer.unpack('C5')
      packet = create_from_header(bytes.first, version)
      packet.validate_flags

      # Parse the packet length
      body_length = 0
      multiplier = 1
      pos = 1

      loop do
        if buffer.bytesize <= pos
          raise ProtocolException, 'The packet length header is incomplete'
        end

        digit = bytes[pos]
        body_length += ((digit & 0x7F) * multiplier)
        multiplier *= 0x80
        pos += 1
        break if (digit & 0x80).zero? || pos > 4
      end

      # Store the expected body length in the packet
      packet.instance_variable_set('@body_length', body_length)

      # Delete the fixed header from the raw packet passed in
      buffer.slice!(0...pos)

      packet
    end

    # Create a new packet object from the first byte of a MQTT packet
    def self.create_from_header(byte, version = nil)
      # Work out the class
      type_id = ((byte & 0xF0) >> 4)
      packet_class = MQTT::PACKET_TYPES[type_id]
      if packet_class.nil?
        raise ProtocolException, "Invalid packet type identifier: #{type_id}"
      end

      # Convert the last 4 bits of byte into array of true/false
      flags = (0..3).map { |i| byte & (2**i) != 0 }

      # Create a new packet object
      args = { :flags => flags }
      args[:version] = version unless version.nil?
      packet_class.new(args)
    end

    # Create a new empty packet
    def initialize(args = {})
      # We must set flags before the other values
      @flags = [false, false, false, false]
      update_attributes(ATTR_DEFAULTS.merge(args))
    end

    # Set packet attributes from a hash of attribute names and values
    def update_attributes(attr = {})
      attr.each_pair do |k, v|
        if v.is_a?(Array) || v.is_a?(Hash)
          send("#{k}=", v.dup)
        else
          send("#{k}=", v)
        end
      end
    end

    # Get the identifer for this packet type
    def type_id
      index = MQTT::PACKET_TYPES.index(self.class)
      raise "Invalid packet type: #{self.class}" if index.nil?
      index
    end

    # Get the name of the packet type as a string in capitals
    # (like the MQTT specification uses)
    #
    # Example: CONNACK
    def type_name
      self.class.name.split('::').last.upcase
    end

    # Set the protocol version number
    def version=(arg)
      @version = arg.to_s == '5' ? '5.0' : arg.to_s
    end

    # Returns true if the packet is using MQTT version 5
    def mqtt5?
      @version == '5.0'
    end

    # Set the length of the packet body
    def body_length=(arg)
      @body_length = arg.to_i
    end

    # Parse the body (variable header and payload) of a packet
    def parse_body(buffer)
      return if buffer.bytesize == body_length

      raise ProtocolException, "Failed to parse packet - input buffer (#{buffer.bytesize}) is not the same as the body length header (#{body_length})"
    end

    # Get serialisation of packet's body (variable header and payload)
    def encode_body
      '' # No body by default
    end

    # Serialise the packet
    def to_s
      # Encode the fixed header
      header = [
        ((type_id.to_i & 0x0F) << 4) |
          (flags[3] ? 0x8 : 0x0) |
          (flags[2] ? 0x4 : 0x0) |
          (flags[1] ? 0x2 : 0x0) |
          (flags[0] ? 0x1 : 0x0)
      ]

      # Get the packet's variable header and payload
      body = encode_body

      # Check that that packet isn't too big
      body_length = body.bytesize
      if body_length > 268_435_455
        raise 'Error serialising packet: body is more than 256MB'
      end

      # Build up the body length field bytes
      loop do
        digit = (body_length % 128)
        body_length = body_length.div(128)
        # if there are more digits to encode, set the top bit of this digit
        digit |= 0x80 if body_length > 0
        header.push(digit)
        break if body_length <= 0
      end

      # Convert header to binary and add on body
      header.pack('C*') + body
    end

    # Check that fixed header flags are valid for types that don't use the flags
    # @private
    def validate_flags
      return if flags == [false, false, false, false]

      raise ProtocolException, "Invalid flags in #{type_name} packet header"
    end

    # Returns a human readable string
    def inspect
      "\#<#{self.class}>"
    end

    # Read and unpack a single byte from a socket
    def self.read_byte(socket)
      byte = socket.getbyte
      raise ProtocolException, 'Failed to read byte from socket' if byte.nil?

      byte
    end

    protected

    # Encode an array of bytes and return them
    def encode_bytes(*bytes)
      bytes.pack('C*')
    end

    # Encode an array of bits and return them
    def encode_bits(bits)
      [bits.map { |b| b ? '1' : '0' }.join].pack('b*')
    end

    # Encode a 16-bit unsigned integer and return it
    def encode_short(val)
      raise 'Value too big for short' if val > 0xffff
      [val.to_i].pack('n')
    end

    # Encode a UTF-8 string and return it
    # (preceded by the length of the string)
    def encode_string(str)
      str = str.to_s.encode('UTF-8')

      # Force to binary, when assembling the packet
      str.force_encoding('ASCII-8BIT')
      encode_short(str.bytesize) + str
    end

    # Remove a 16-bit unsigned integer from the front of buffer
    def shift_short(buffer)
      bytes = buffer.slice!(0..1)
      bytes.unpack('n').first
    end

    # Remove one byte from the front of the string
    def shift_byte(buffer)
      buffer.slice!(0...1).unpack('C').first
    end

    # Remove 8 bits from the front of buffer
    def shift_bits(buffer)
      buffer.slice!(0...1).unpack('b8').first.split('').map { |b| b == '1' }
    end

    # Remove n bytes from the front of buffer
    def shift_data(buffer, bytes)
      buffer.slice!(0...bytes)
    end

    # Remove string from the front of buffer
    def shift_string(buffer)
      len = shift_short(buffer)
      str = shift_data(buffer, len)
      # Strings in MQTT v3.1 are all UTF-8
      str.force_encoding('UTF-8')
    end

    # Encode a 32-bit unsigned integer and return it
    def encode_long(val)
      [val.to_i].pack('N')
    end

    # Remove a 32-bit unsigned integer from the front of buffer
    def shift_long(buffer)
      buffer.slice!(0..3).unpack('N').first
    end

    # Encode a variable byte integer and return it
    def encode_variable_byte(val)
      raise ProtocolException, 'Variable byte integer too large' if val > 268_435_455

      bytes = ''
      loop do
        digit = val % 128
        val = val.div(128)
        digit |= 0x80 if val > 0
        bytes += encode_bytes(digit)
        break if val.zero?
      end
      bytes
    end

    # Remove a variable byte integer from the front of buffer
    def shift_variable_byte(buffer)
      value = 0
      4.times do |pos|
        digit = buffer.empty? ? nil : shift_byte(buffer)
        raise ProtocolException, 'Truncated variable byte integer' if digit.nil?

        value = value + ((digit & 0x7F) * (128 ** pos))
        return value if (digit & 0x80).zero?
      end
      raise ProtocolException, 'Malformed variable byte integer'
    end

    # Encode a hash of MQTT 5 properties and return it
    def encode_properties(properties)
      body = properties.map do |name, value|
        id = PROPERTY_IDS.fetch(name) do
          raise ProtocolException, "Unknown MQTT 5 property: #{name.inspect}"
        end
        Array(value).map do |item|
          encode_bytes(id) + encode_property(PROPERTY_TYPES[id][1], item)
        end.join
      end.join
      encode_variable_byte(body.bytesize) + body
    end

    # Encode a single MQTT 5 property value
    def encode_property(type, value)
      case type
      when :byte then encode_bytes(value.to_i)
      when :short then encode_short(value)
      when :long then encode_long(value)
      when :variable_integer then encode_variable_byte(value.to_i)
      when :utf8 then encode_string(value)
      when :binary
        data = value.to_s.dup.force_encoding('ASCII-8BIT')
        encode_short(data.bytesize) + data
      when :utf8_pair
        encode_string(value[0]) + encode_string(value[1])
      else
        raise ProtocolException, "Unsupported MQTT 5 property type: #{type}"
      end
    end

    # Parse MQTT 5 properties from the front of the buffer
    def shift_properties(buffer)
      properties = {}
      length = shift_variable_byte(buffer)
      data = shift_data(buffer, length)
      until data.empty?
        id = shift_byte(data)
        type = PROPERTY_TYPES.fetch(id) do
          raise ProtocolException, "Unknown MQTT 5 property identifier: #{id}"
        end
        name = type[0]
        value = shift_property(type[1], data)
        if MULTI_VALUE_PROPERTIES.include?(name)
          properties[name] ||= []
          properties[name] << value
        elsif properties.key?(name)
          raise ProtocolException, "MQTT 5 property #{name.inspect} appears more than once"
        else
          properties[name] = value
        end
      end
      properties
    end

    # Parse a single MQTT 5 property value
    def shift_property(type, buffer)
      case type
      when :byte then shift_byte(buffer)
      when :short then shift_short(buffer)
      when :long then shift_long(buffer)
      when :variable_integer then shift_variable_byte(buffer)
      when :utf8 then shift_string(buffer)
      when :binary then shift_data(buffer, shift_short(buffer))
      when :utf8_pair then [shift_string(buffer), shift_string(buffer)]
      else
        raise ProtocolException, "Unsupported MQTT 5 property type: #{type}"
      end
    end

    # Parse the optional MQTT 5 reason code and properties of an acknowledgement
    def parse_remaining_reason(buffer, name)
      return if buffer.empty?

      unless mqtt5?
        raise ProtocolException, "Extra bytes at end of #{name} packet"
      end

      @reason_code = shift_byte(buffer)
      @properties = shift_properties(buffer)

      return if buffer.empty?
      raise ProtocolException, "Extra bytes at end of #{name} packet"
    end

    ## PACKET SUBCLASSES ##

    # Class representing an MQTT Publish message
    class Publish < MQTT::Packet
      # Duplicate delivery flag
      attr_accessor :duplicate

      # Retain flag
      attr_accessor :retain

      # Quality of Service level (0, 1, 2)
      attr_accessor :qos

      # The topic name to publish to
      attr_accessor :topic

      # The data to be published
      attr_accessor :payload

      # Default attribute values
      ATTR_DEFAULTS = {
        :topic => nil,
        :payload => ''
      }

      # Create a new Publish packet
      def initialize(args = {})
        super(ATTR_DEFAULTS.merge(args))
      end

      def duplicate
        @flags[3]
      end

      # Set the DUP flag (true/false)
      def duplicate=(arg)
        @flags[3] = arg.is_a?(Integer) ? (arg == 0x1) : arg
      end

      def retain
        @flags[0]
      end

      # Set the retain flag (true/false)
      def retain=(arg)
        @flags[0] = arg.is_a?(Integer) ? (arg == 0x1) : arg
      end

      def qos
        (@flags[1] ? 0x01 : 0x00) | (@flags[2] ? 0x02 : 0x00)
      end

      # Set the Quality of Service level (0/1/2)
      def qos=(arg)
        @qos = arg.to_i
        raise "Invalid QoS value: #{@qos}" if @qos < 0 || @qos > 2

        @flags[1] = (arg & 0x01 == 0x01)
        @flags[2] = (arg & 0x02 == 0x02)
      end

      # Get serialisation of packet's body
      def encode_body
        body = ''
        if @topic.nil? || @topic.to_s.empty?
          raise 'Invalid topic name when serialising packet'
        end
        body += encode_string(@topic)
        body += encode_short(@id) unless qos.zero?
        body += encode_properties(@properties) if mqtt5?
        body += payload.to_s.dup.force_encoding('ASCII-8BIT')
        body
      end

      # Parse the body (variable header and payload) of a Publish packet
      def parse_body(buffer)
        super(buffer)
        @topic = shift_string(buffer)
        @id = shift_short(buffer) unless qos.zero?
        @properties = shift_properties(buffer) if mqtt5?
        @payload = buffer
      end

      # MQTT 5 subscription identifiers returned by the server (always an array)
      def subscription_identifiers
        @properties[:subscription_identifier] || []
      end

      # Check that fixed header flags are valid for this packet type
      # @private
      def validate_flags
        raise ProtocolException, 'Invalid packet: QoS value of 3 is not allowed' if qos == 3
        raise ProtocolException, 'Invalid packet: DUP cannot be set for QoS 0' if qos.zero? && duplicate
      end

      # Returns a human readable string, summarising the properties of the packet
      def inspect
        "\#<#{self.class}: " \
          "d#{duplicate ? '1' : '0'}, " \
          "q#{qos}, " \
          "r#{retain ? '1' : '0'}, " \
          "m#{id}, " \
          "'#{topic}', " \
          "#{inspect_payload}>"
      end

      protected

      def inspect_payload
        str = payload.to_s
        if str.bytesize < 16 && str =~ /^[ -~]*$/
          "'#{str}'"
        else
          "... (#{str.bytesize} bytes)"
        end
      end
    end

    # Class representing an MQTT Connect Packet
    class Connect < MQTT::Packet
      # The name of the protocol
      attr_accessor :protocol_name

      # The version number of the protocol
      attr_accessor :protocol_level

      # The client identifier string
      attr_accessor :client_id

      # Set to false to keep a persistent session with the server
      attr_accessor :clean_session

      # Period the server should keep connection open for between pings
      attr_accessor :keep_alive

      # The topic name to send the Will message to
      attr_accessor :will_topic

      # The QoS level to send the Will message as
      attr_accessor :will_qos

      # Set to true to make the Will message retained
      attr_accessor :will_retain

      # The payload of the Will message
      attr_accessor :will_payload

      # MQTT 5 properties associated with the Will message
      attr_accessor :will_properties

      # The username for authenticating with the server
      attr_accessor :username

      # The password for authenticating with the server
      attr_accessor :password

      # Default attribute values
      ATTR_DEFAULTS = {
        :client_id => nil,
        :clean_session => true,
        :keep_alive => 15,
        :will_topic => nil,
        :will_qos => 0,
        :will_retain => false,
        :will_payload => '',
        :will_properties => {},
        :username => nil,
        :password => nil
      }

      # Create a new Client Connect packet
      def initialize(args = {})
        super(ATTR_DEFAULTS.merge(args))

        if ['3.1.0', '3.1'].include?(version)
          self.protocol_name ||= 'MQIsdp'
          self.protocol_level ||= 0x03
        elsif version == '3.1.1'
          self.protocol_name ||= 'MQTT'
          self.protocol_level ||= 0x04
        elsif version == '5.0'
          self.protocol_name ||= 'MQTT'
          self.protocol_level ||= 0x05
        else
          raise ArgumentError, "Unsupported protocol version: #{version}"
        end
      end

      # Get serialisation of packet's body
      def encode_body
        body = ''

        if @version == '3.1.0'
          raise 'Client identifier too short while serialising packet' if @client_id.nil? || @client_id.bytesize < 1
          raise 'Client identifier too long when serialising packet' if @client_id.bytesize > 23
        end

        body += encode_string(@protocol_name)
        body += encode_bytes(@protocol_level.to_i)

        if @keep_alive < 0
          raise 'Invalid keep-alive value: cannot be less than 0'
        end

        # Set the Connect flags
        @connect_flags = 0
        @connect_flags |= 0x02 if @clean_session
        @connect_flags |= 0x04 unless @will_topic.nil?
        if mqtt5?
          @connect_flags |= ((@will_qos & 0x03) << 3)
          @connect_flags |= 0x20 if @will_retain
          @connect_flags |= 0x40 unless @password.nil?
          @connect_flags |= 0x80 unless @username.nil?
          body += encode_bytes(@connect_flags)
          body += encode_short(@keep_alive)
          body += encode_properties(@properties)
        else
          @connect_flags |= ((@will_qos & 0x03) << 3)
          @connect_flags |= 0x20 if @will_retain
          @connect_flags |= 0x40 unless @password.nil?
          @connect_flags |= 0x80 unless @username.nil?
          body += encode_bytes(@connect_flags)
          body += encode_short(@keep_alive)
        end
        body += encode_string(@client_id)
        unless will_topic.nil?
          body += encode_properties(@will_properties) if mqtt5?
          body += encode_string(@will_topic)
          if mqtt5?
            data = @will_payload.to_s.dup.force_encoding('ASCII-8BIT')
            body += encode_short(data.bytesize) + data
          else
            # The MQTT v3.1 specification says that the payload is a UTF-8 string
            body += encode_string(@will_payload)
          end
        end
        body += encode_string(@username) unless @username.nil?
        unless @password.nil?
          if mqtt5?
            data = @password.to_s.dup.force_encoding('ASCII-8BIT')
            body += encode_short(data.bytesize) + data
          else
            body += encode_string(@password)
          end
        end
        body
      end

      # Parse the body (variable header and payload) of a Connect packet
      def parse_body(buffer)
        super(buffer)
        @protocol_name = shift_string(buffer)
        @protocol_level = shift_byte(buffer).to_i
        if @protocol_name == 'MQIsdp' && @protocol_level == 3
          @version = '3.1.0'
        elsif @protocol_name == 'MQTT' && @protocol_level == 4
          @version = '3.1.1'
        elsif @protocol_name == 'MQTT' && @protocol_level == 5
          @version = '5.0'
        else
          raise ProtocolException, "Unsupported protocol: #{@protocol_name}/#{@protocol_level}"
        end

        @connect_flags = shift_byte(buffer)
        @clean_session = ((@connect_flags & 0x02) >> 1) == 0x01
        @keep_alive = shift_short(buffer)
        @properties = shift_properties(buffer) if mqtt5?
        @client_id = shift_string(buffer)
        if ((@connect_flags & 0x04) >> 2) == 0x01
          # Last Will and Testament
          @will_qos = ((@connect_flags & 0x18) >> 3)
          if mqtt5?
            @will_properties = shift_properties(buffer)
            @will_retain = ((@connect_flags & 0x20) >> 5) == 0x01
          else
            @will_retain = ((@connect_flags & 0x20) >> 5) == 0x01
          end
          @will_topic = shift_string(buffer)
          if mqtt5?
            @will_payload = shift_data(buffer, shift_short(buffer))
          else
            # The MQTT v3.1 specification says that the payload is a UTF-8 string
            @will_payload = shift_string(buffer)
          end
        end
        if ((@connect_flags & 0x80) >> 7) == 0x01 && buffer.bytesize > 0
          @username = shift_string(buffer)
        end
        if ((@connect_flags & 0x40) >> 6) == 0x01 && buffer.bytesize > 0 # rubocop: disable Style/GuardClause
          if mqtt5?
            @password = shift_data(buffer, shift_short(buffer))
          else
            @password = shift_string(buffer)
          end
        end
      end

      # Returns a human readable string, summarising the properties of the packet
      def inspect
        str = "\#<#{self.class}: " \
              "keep_alive=#{keep_alive}"
        str += ', clean' if clean_session
        str += ", client_id='#{client_id}'"
        str += ", username='#{username}'" unless username.nil?
        str += ', password=...' unless password.nil?
        str + '>'
      end

      # ---- Deprecated attributes and methods  ---- #

      # @deprecated Please use {#protocol_level} instead
      def protocol_version
        protocol_level
      end

      # @deprecated Please use {#protocol_level=} instead
      def protocol_version=(args)
        self.protocol_level = args
      end
    end

    # Class representing an MQTT Connect Acknowledgment Packet
    class Connack < MQTT::Packet
      # Session Present flag
      attr_accessor :session_present

      # The return code (defaults to 0 for connection accepted)
      attr_accessor :return_code

      # MQTT 5 reason code (an alias of return_code)
      alias_method :reason_code, :return_code
      alias_method :reason_code=, :return_code=

      # Default attribute values
      ATTR_DEFAULTS = { :return_code => 0x00 }

      # Create a new Client Connect packet
      def initialize(args = {})
        # We must set flags before other attributes
        @connack_flags = [false, false, false, false, false, false, false, false]
        super(ATTR_DEFAULTS.merge(args))
      end

      # Get the Session Present flag
      def session_present
        @connack_flags[0]
      end

      # Set the Session Present flag
      def session_present=(arg)
        @connack_flags[0] = arg.is_a?(Integer) ? (arg == 0x1) : arg
      end

      # Get a string message corresponding to a return code
      def return_msg
        case return_code
        when 0x00
          'Connection Accepted'
        when 0x01
          'Connection refused: unacceptable protocol version'
        when 0x02
          'Connection refused: client identifier rejected'
        when 0x03
          'Connection refused: server unavailable'
        when 0x04
          'Connection refused: bad user name or password'
        when 0x05
          'Connection refused: not authorised'
        else
          "Connection refused: error code #{return_code}"
        end
      end

      # Get serialisation of packet's body
      def encode_body
        body = ''
        body += encode_bits(@connack_flags)
        body += encode_bytes(@return_code.to_i)
        body += encode_properties(@properties) if mqtt5?
        body
      end

      # Parse the body (variable header and payload) of a Connect Acknowledgment packet
      def parse_body(buffer)
        super(buffer)
        @connack_flags = shift_bits(buffer)
        unless @connack_flags[1, 7] == [false, false, false, false, false, false, false]
          raise ProtocolException, 'Invalid flags in Connack variable header'
        end
        @return_code = shift_byte(buffer)

        # Without an explicit version a Connack is always exactly 2 bytes
        if mqtt5?
          @properties = shift_properties(buffer)
        elsif buffer.bytesize > 0
          raise ProtocolException, 'Extra bytes at end of Connect Acknowledgment packet'
        end

        return if buffer.empty?
        raise ProtocolException, 'Extra bytes at end of Connect Acknowledgment packet'
      end

      # Returns a human readable string, summarising the properties of the packet
      def inspect
        "\#<#{self.class}: 0x%2.2X>" % return_code
      end
    end

    # Class representing an MQTT Publish Acknowledgment packet
    class Puback < MQTT::Packet
      # MQTT 5 reason code
      attr_accessor :reason_code

      ATTR_DEFAULTS = {
        :reason_code => 0
      }

      # Get serialisation of packet's body
      def encode_body
        body = encode_short(@id)
        if mqtt5? && (@reason_code.to_i != 0 || !@properties.empty?)
          body += encode_bytes(@reason_code.to_i)
          body += encode_properties(@properties)
        end
        body
      end

      # Parse the body (variable header and payload) of a packet
      def parse_body(buffer)
        super(buffer)
        @id = shift_short(buffer)

        parse_remaining_reason(buffer, 'Publish Acknowledgment')
      end

      # Returns a human readable string, summarising the properties of the packet
      def inspect
        "\#<#{self.class}: 0x%2.2X>" % id
      end
    end

    # Class representing an MQTT Publish Received packet
    class Pubrec < MQTT::Packet
      # MQTT 5 reason code
      attr_accessor :reason_code

      ATTR_DEFAULTS = {
        :reason_code => 0
      }

      # Get serialisation of packet's body
      def encode_body
        body = encode_short(@id)
        if mqtt5? && (@reason_code.to_i != 0 || !@properties.empty?)
          body += encode_bytes(@reason_code.to_i)
          body += encode_properties(@properties)
        end
        body
      end

      # Parse the body (variable header and payload) of a packet
      def parse_body(buffer)
        super(buffer)
        @id = shift_short(buffer)

        parse_remaining_reason(buffer, 'Publish Received')
      end

      # Returns a human readable string, summarising the properties of the packet
      def inspect
        "\#<#{self.class}: 0x%2.2X>" % id
      end
    end

    # Class representing an MQTT Publish Release packet
    class Pubrel < MQTT::Packet
      # MQTT 5 reason code
      attr_accessor :reason_code

      # Default attribute values
      ATTR_DEFAULTS = {
        :flags => [false, true, false, false],
        :reason_code => 0
      }

      # Create a new Pubrel packet
      def initialize(args = {})
        super(ATTR_DEFAULTS.merge(args))
      end

      # Get serialisation of packet's body
      def encode_body
        body = encode_short(@id)
        return body unless mqtt5?

        # PUBREL uses reason code 0x10 when no reason code or properties are sent
        code = @reason_code.to_i
        code = 0x10 if code.zero? && @properties.empty?
        body + encode_bytes(code) + encode_properties(@properties)
      end

      # Parse the body (variable header and payload) of a packet
      def parse_body(buffer)
        super(buffer)
        @id = shift_short(buffer)

        parse_remaining_reason(buffer, 'Publish Release')
      end

      # Check that fixed header flags are valid for this packet type
      # @private
      def validate_flags
        return if @flags == [false, true, false, false]
        raise ProtocolException, 'Invalid flags in PUBREL packet header'
      end

      # Returns a human readable string, summarising the properties of the packet
      def inspect
        "\#<#{self.class}: 0x%2.2X>" % id
      end
    end

    # Class representing an MQTT Publish Complete packet
    class Pubcomp < MQTT::Packet
      # MQTT 5 reason code
      attr_accessor :reason_code

      ATTR_DEFAULTS = {
        :reason_code => 0
      }

      # Get serialisation of packet's body
      def encode_body
        body = encode_short(@id)
        if mqtt5? && (@reason_code.to_i != 0 || !@properties.empty?)
          body += encode_bytes(@reason_code.to_i)
          body += encode_properties(@properties)
        end
        body
      end

      # Parse the body (variable header and payload) of a packet
      def parse_body(buffer)
        super(buffer)
        @id = shift_short(buffer)

        parse_remaining_reason(buffer, 'Publish Complete')
      end

      # Returns a human readable string, summarising the properties of the packet
      def inspect
        "\#<#{self.class}: 0x%2.2X>" % id
      end
    end

    # Class representing an MQTT Client Subscribe packet
    class Subscribe < MQTT::Packet
      # One or more topic filters to subscribe to
      attr_accessor :topics

      # MQTT 5 subscription identifier associated with all filters in this packet
      def subscription_identifier
        @properties[:subscription_identifier]&.first
      end

      def subscription_identifier=(identifier)
        identifier = identifier.to_i
        raise ProtocolException, 'Subscription identifier must be between 1 and 268435455' unless identifier.between?(1, 268_435_455)

        @properties[:subscription_identifier] = [identifier]
      end

      # Default attribute values
      ATTR_DEFAULTS = {
        :topics => [],
        :flags => [false, true, false, false]
      }

      # Create a new Subscribe packet
      def initialize(args = {})
        super(ATTR_DEFAULTS.merge(args))
      end

      # Set one or more topic filters for the Subscribe packet
      # The topics parameter should be one of the following:
      # * String: subscribe to one topic with QoS 0
      # * Array: subscribe to multiple topics with QoS 0
      # * Hash: subscribe to multiple topics where the key is the topic and the value is the QoS level
      #
      # For example:
      #   packet.topics = 'a/b'
      #   packet.topics = ['a/b', 'c/d']
      #   packet.topics = [['a/b',0], ['c/d',1]]
      #   packet.topics = {'a/b' => 0, 'c/d' => 1}
      #
      def topics=(value)
        # Get input into a consistent state
        input = value.is_a?(Array) ? value.flatten : [value]

        @topics = []
        until input.empty?
          item = input.shift
          if item.is_a?(Hash)
            # Convert hash into an ordered array of arrays
            @topics += item.sort
          elsif item.is_a?(String)
            # Peek at the next item in the array, and remove it if it is an integer
            if input.first.is_a?(Integer)
              qos = input.shift
              @topics << [item, qos]
            else
              @topics << [item, 0]
            end
          else
            # Meh?
            raise "Invalid topics input: #{value.inspect}"
          end
        end
        @topics
      end

      # Get serialisation of packet's body
      def encode_body
        raise 'no topics given when serialising packet' if @topics.empty?
        body = encode_short(@id)
        body += encode_properties(@properties) if mqtt5?
        topics.each do |item|
          body += encode_string(item[0])
          body += encode_bytes(mqtt5? ? (item[1].to_i & 0x7F) : item[1])
        end
        body
      end

      # Parse the body (variable header and payload) of a packet
      def parse_body(buffer)
        super(buffer)
        @id = shift_short(buffer)
        @properties = shift_properties(buffer) if mqtt5?
        @topics = []
        while buffer.bytesize > 0
          topic_name = shift_string(buffer)
          topic_options = shift_byte(buffer)
          topic_qos = mqtt5? ? (topic_options & 0x03) : topic_options
          @topics << [topic_name, topic_qos]
        end
      end

      # Check that fixed header flags are valid for this packet type
      # @private
      def validate_flags
        return if @flags == [false, true, false, false]
        raise ProtocolException, 'Invalid flags in SUBSCRIBE packet header'
      end

      # Returns a human readable string, summarising the properties of the packet
      def inspect
        _str = "\#<#{self.class}: 0x%2.2X, %s>" % [
          id,
          topics.map { |t| "'#{t[0]}':#{t[1]}" }.join(', ')
        ]
      end
    end

    # Class representing an MQTT Subscribe Acknowledgment packet
    class Suback < MQTT::Packet
      # An array of return codes, ordered by the topics that were subscribed to
      attr_accessor :return_codes

      # Default attribute values
      ATTR_DEFAULTS = {
        :return_codes => []
      }

      # Create a new Subscribe Acknowledgment packet
      def initialize(args = {})
        super(ATTR_DEFAULTS.merge(args))
      end

      # Set the granted QoS value for each of the topics that were subscribed to
      # Can either be an integer or an array or integers.
      def return_codes=(value)
        if value.is_a?(Array)
          @return_codes = value
        elsif value.is_a?(Integer)
          @return_codes = [value]
        else
          raise 'return_codes should be an integer or an array of return codes'
        end
      end

      # Get serialisation of packet's body
      def encode_body
        if @return_codes.empty?
          raise 'no granted QoS given when serialising packet'
        end
        body = encode_short(@id)
        body += encode_properties(@properties) if mqtt5?
        return_codes.each { |qos| body += encode_bytes(qos) }
        body
      end

      # Parse the body (variable header and payload) of a packet
      def parse_body(buffer)
        super(buffer)
        @id = shift_short(buffer)
        if mqtt5?
          @properties = shift_properties(buffer)
        end
        @return_codes << shift_byte(buffer) while buffer.bytesize > 0
      end

      # Returns a human readable string, summarising the properties of the packet
      def inspect
        "\#<#{self.class}: 0x%2.2X, rc=%s>" % [id, return_codes.map { |rc| '0x%2.2X' % rc }.join(',')]
      end

      # ---- Deprecated attributes and methods  ---- #

      # @deprecated Please use {#return_codes} instead
      def granted_qos
        return_codes
      end

      # @deprecated Please use {#return_codes=} instead
      def granted_qos=(args)
        self.return_codes = args
      end
    end

    # Class representing an MQTT Client Unsubscribe packet
    class Unsubscribe < MQTT::Packet
      # One or more topic paths to unsubscribe from
      attr_accessor :topics

      # Default attribute values
      ATTR_DEFAULTS = {
        :topics => [],
        :flags => [false, true, false, false]
      }

      # Create a new Unsubscribe packet
      def initialize(args = {})
        super(ATTR_DEFAULTS.merge(args))
      end

      # Set one or more topic paths to unsubscribe from
      def topics=(value)
        @topics = value.is_a?(Array) ? value : [value]
      end

      # Get serialisation of packet's body
      def encode_body
        raise 'no topics given when serialising packet' if @topics.empty?
        body = encode_short(@id)
        body += encode_properties(@properties) if mqtt5?
        topics.each { |topic| body += encode_string(topic) }
        body
      end

      # Parse the body (variable header and payload) of a packet
      def parse_body(buffer)
        super(buffer)
        @id = shift_short(buffer)
        @properties = shift_properties(buffer) if mqtt5?
        @topics << shift_string(buffer) while buffer.bytesize > 0
      end

      # Check that fixed header flags are valid for this packet type
      # @private
      def validate_flags
        return if @flags == [false, true, false, false]
        raise ProtocolException, 'Invalid flags in UNSUBSCRIBE packet header'
      end

      # Returns a human readable string, summarising the properties of the packet
      def inspect
        "\#<#{self.class}: 0x%2.2X, %s>" % [
          id,
          topics.map { |t| "'#{t}'" }.join(', ')
        ]
      end
    end

    # Class representing an MQTT Unsubscribe Acknowledgment packet
    class Unsuback < MQTT::Packet
      # MQTT 5 reason codes, one per unsubscribed topic filter
      attr_accessor :reason_codes

      ATTR_DEFAULTS = {
        :reason_codes => []
      }

      # Create a new Unsubscribe Acknowledgment packet
      def initialize(args = {})
        super(args)
      end

      # Get serialisation of packet's body
      def encode_body
        body = encode_short(@id)
        if mqtt5?
          body += encode_properties(@properties)
          reason_codes.each { |code| body += encode_bytes(code) }
        end
        body
      end

      # Parse the body (variable header and payload) of a packet
      def parse_body(buffer)
        super(buffer)
        @id = shift_short(buffer)

        if mqtt5?
          @properties = shift_properties(buffer)
          @reason_codes = []
          @reason_codes << shift_byte(buffer) while buffer.bytesize > 0
          return
        end

        return if buffer.empty?
        raise ProtocolException, 'Extra bytes at end of Unsubscribe Acknowledgment packet'
      end

      # Returns a human readable string, summarising the properties of the packet
      def inspect
        "\#<#{self.class}: 0x%2.2X>" % id
      end
    end

    # Class representing an MQTT Ping Request packet
    class Pingreq < MQTT::Packet
      # Create a new Ping Request packet
      def initialize(args = {})
        super(args)
      end

      # Check the body
      def parse_body(buffer)
        super(buffer)

        return if buffer.empty?
        raise ProtocolException, 'Extra bytes at end of Ping Request packet'
      end
    end

    # Class representing an MQTT Ping Response packet
    class Pingresp < MQTT::Packet
      # Create a new Ping Response packet
      def initialize(args = {})
        super(args)
      end

      # Check the body
      def parse_body(buffer)
        super(buffer)

        return if buffer.empty?
        raise ProtocolException, 'Extra bytes at end of Ping Response packet'
      end
    end

    # Class representing an MQTT Client Disconnect packet
    class Disconnect < MQTT::Packet
      # MQTT 5 reason code
      attr_accessor :reason_code

      ATTR_DEFAULTS = {
        :reason_code => 0
      }

      # Create a new Client Disconnect packet
      def initialize(args = {})
        super(args)
      end

      # Get serialisation of packet's body
      def encode_body
        return '' unless mqtt5?

        body = encode_bytes(@reason_code.to_i)
        body + encode_properties(@properties)
      end

      # Check the body
      def parse_body(buffer)
        super(buffer)

        return if buffer.empty?
        if mqtt5?
          @reason_code = shift_byte(buffer)
          @properties = shift_properties(buffer)

          return if buffer.empty?
          raise ProtocolException, 'Extra bytes at end of Disconnect packet'
        end

        raise ProtocolException, 'Extra bytes at end of Disconnect packet'
      end
    end

    # Class representing an MQTT 5 Authentication Exchange packet
    class Auth < MQTT::Packet
      # MQTT 5 reason code
      attr_accessor :reason_code

      ATTR_DEFAULTS = {
        :reason_code => 0
      }

      # Create a new Authentication packet
      def initialize(args = {})
        super(args)
      end

      # Get serialisation of packet's body
      def encode_body
        return encode_bytes(@reason_code.to_i) + encode_properties(@properties) if @reason_code.to_i != 0

        encode_properties(@properties)
      end

      # Parse the body (variable header and payload) of a packet
      def parse_body(buffer)
        super(buffer)
        return if buffer.empty?

        @reason_code = shift_byte(buffer)
        @properties = shift_properties(buffer)
      end
    end

    # ---- Deprecated attributes and methods  ---- #
    public

    # @deprecated Please use {#id} instead
    def message_id
      id
    end

    # @deprecated Please use {#id=} instead
    def message_id=(args)
      self.id = args
    end
  end

  # An enumeration of the MQTT packet types
  PACKET_TYPES = [
    nil,
    MQTT::Packet::Connect,
    MQTT::Packet::Connack,
    MQTT::Packet::Publish,
    MQTT::Packet::Puback,
    MQTT::Packet::Pubrec,
    MQTT::Packet::Pubrel,
    MQTT::Packet::Pubcomp,
    MQTT::Packet::Subscribe,
    MQTT::Packet::Suback,
    MQTT::Packet::Unsubscribe,
    MQTT::Packet::Unsuback,
    MQTT::Packet::Pingreq,
    MQTT::Packet::Pingresp,
    MQTT::Packet::Disconnect,
    MQTT::Packet::Auth
  ]

  # MQTT 5 property identifiers, mapped to their name and value type
  PROPERTY_TYPES = {
    1 => [:payload_format_indicator, :byte],
    2 => [:message_expiry_interval, :long],
    3 => [:content_type, :utf8],
    8 => [:response_topic, :utf8],
    9 => [:correlation_data, :binary],
    11 => [:subscription_identifier, :variable_integer],
    17 => [:session_expiry_interval, :long],
    18 => [:assigned_client_identifier, :utf8],
    19 => [:server_keep_alive, :short],
    21 => [:authentication_method, :utf8],
    22 => [:authentication_data, :binary],
    23 => [:request_problem_information, :byte],
    24 => [:will_delay_interval, :long],
    25 => [:request_response_information, :byte],
    26 => [:response_information, :utf8],
    28 => [:server_reference, :utf8],
    31 => [:reason_string, :utf8],
    33 => [:receive_maximum, :short],
    34 => [:topic_alias_maximum, :short],
    35 => [:topic_alias, :short],
    36 => [:maximum_qos, :byte],
    37 => [:retain_available, :byte],
    38 => [:user_property, :utf8_pair],
    39 => [:maximum_packet_size, :long],
    40 => [:wildcard_subscription_available, :byte],
    41 => [:subscription_identifier_available, :byte],
    42 => [:shared_subscription_available, :byte]
  }.freeze

  # MQTT 5 property names mapped to their identifiers
  PROPERTY_IDS = PROPERTY_TYPES.each_with_object({}) do |(id, type), map|
    map[type[0]] = id
  end.freeze

  # MQTT 5 properties that may appear more than once in a packet
  MULTI_VALUE_PROPERTIES = [:user_property, :subscription_identifier].freeze
end

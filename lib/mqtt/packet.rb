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

    # Default attribute values
    ATTR_DEFAULTS = {
      :version => '3.1.0',
      :id => 0,
      :body_length => nil
    }

    # Read in a packet from a socket.
    # The protocol version of the connection can be passed in so that
    # MQTT 5 bodies (with reason codes and properties) are parsed correctly.
    def self.read(socket, version = nil)
      # Read in the packet header and create a new packet object
      packet = create_from_header(
        read_byte(socket)
      )
      packet.version = version unless version.nil?
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

    # Parse buffer into new packet object.
    # The protocol version of the connection can be passed in so that
    # MQTT 5 bodies (with reason codes and properties) are parsed correctly.
    def self.parse(buffer, version = nil)
      packet = parse_header(buffer)
      packet.version = version unless version.nil?
      packet.parse_body(buffer)
      packet
    end

    # Parse the header and create a new packet object of the correct type
    # The header is removed from the buffer passed into this function
    def self.parse_header(buffer)
      # Check that the packet is a long as the minimum packet size
      if buffer.bytesize < 2
        raise ProtocolException, 'Invalid packet: less than 2 bytes long'
      end

      # Create a new packet object
      bytes = buffer.unpack('C5')
      packet = create_from_header(bytes.first)
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
    def self.create_from_header(byte)
      # Work out the class
      type_id = ((byte & 0xF0) >> 4)
      packet_class = MQTT::PACKET_TYPES[type_id]
      if packet_class.nil?
        raise ProtocolException, "Invalid packet type identifier: #{type_id}"
      end

      # Convert the last 4 bits of byte into array of true/false
      flags = (0..3).map { |i| byte & (2**i) != 0 }

      # Create a new packet object
      packet_class.new(:flags => flags)
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
      @version = case arg.to_s
                 when '5', '5.0' then '5.0'
                 else arg.to_s
                 end
    end

    # Returns true when the packet is using the MQTT 5 protocol
    def mqtt5?
      version == '5.0'
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

    # Encode a Variable Byte Integer (used by MQTT 5)
    def encode_variable_int(value)
      value = value.to_i
      raise 'Variable byte integer cannot be negative' if value.negative?
      raise 'Variable byte integer cannot be greater than 268435455' if value > 268_435_455

      bytes = []
      loop do
        byte = value % 128
        value /= 128
        byte |= 0x80 if value > 0
        bytes << byte
        break if value.zero?
      end
      encode_bytes(*bytes)
    end

    # Remove a Variable Byte Integer from the front of the buffer
    def shift_variable_int(buffer)
      multiplier = 1
      value = 0
      pos = 0
      loop do
        raise ProtocolException, 'Malformed variable byte integer' if pos >= 4
        byte = shift_byte(buffer)
        value += (byte & 0x7F) * multiplier
        return value if (byte & 0x80).zero?
        multiplier *= 128
        pos += 1
      end
    end

    # Encode the MQTT 5 properties hash and return the bytes
    # (including the length of the properties)
    def encode_properties(properties)
      body = properties_to_s(properties)
      encode_variable_int(body.bytesize) + body
    end

    # Parse an MQTT 5 properties section from the buffer into a hash
    def shift_properties(buffer)
      length = shift_variable_int(buffer)
      section = shift_data(buffer, length)
      Properties.parse(section)
    end

    def properties_to_s(properties)
      body = ''.dup
      (properties || {}).each_pair do |key, value|
        id, type = Properties.lookup(key)
        Array(value).each { |item| body << Properties.encode(id, type, item) }
      end
      body.force_encoding('ASCII-8BIT')
    end

    # Codec for the MQTT 5 Properties field
    module Properties
      # Mapping of property name/identifier symbol to wire identifier and type
      DEFINITIONS = [
        [:payload_format_indicator,        0x01, :byte],
        [:message_expiry_interval,         0x02, :int],
        [:content_type,                    0x03, :string],
        [:response_topic,                  0x08, :string],
        [:correlation_data,                0x09, :binary],
        [:subscription_identifier,         0x0B, :variable_int],
        [:session_expiry_interval,         0x11, :int],
        [:assigned_client_identifier,      0x12, :string],
        [:server_keep_alive,               0x13, :short],
        [:authentication_method,           0x15, :string],
        [:authentication_data,             0x16, :binary],
        [:request_problem_information,     0x17, :byte],
        [:will_delay_interval,             0x18, :int],
        [:request_response_information,     0x19, :byte],
        [:response_information,            0x1A, :string],
        [:server_reference,                0x1C, :string],
        [:reason_string,                   0x1F, :string],
        [:receive_maximum,                 0x21, :short],
        [:topic_alias_maximum,             0x22, :short],
        [:topic_alias,                     0x23, :short],
        [:maximum_qos,                     0x24, :byte],
        [:retain_available,                0x25, :byte],
        [:user_property,                   0x26, :string_pair],
        [:maximum_packet_size,             0x27, :int],
        [:wildcard_subscription_available, 0x28, :byte],
        [:subscription_identifier_available, 0x29, :byte],
        [:shared_subscription_available,   0x2A, :byte]
      ].freeze

      BY_NAME = {}.tap do |hash|
        DEFINITIONS.each { |name, id, type| hash[name] = [id, type] }
      end.freeze

      BY_ID = {}.tap do |hash|
        DEFINITIONS.each { |name, id, type| hash[id] = [name, type] }
      end.freeze

      # Property names that may occur more than once in a packet
      MULTI_VALUED = %i[subscription_identifier user_property].freeze

      def self.lookup(key)
        return BY_NAME.fetch(key.to_sym) if key.is_a?(Symbol) || key.is_a?(String)
        BY_ID.fetch(key)
      end

      def self.encode(id, type, value)
        name, = BY_ID.fetch(id)
        [id].pack('C') + encode_value(name, type, value)
      end

      def self.encode_value(_name, type, value)
        case type
        when :byte then [value.to_i].pack('C')
        when :short then [value.to_i].pack('n')
        when :int then [value.to_i].pack('N')
        when :variable_int
          bytes = []
          number = value.to_i
          loop do
            byte = number % 128
            number /= 128
            byte |= 0x80 if number > 0
            bytes << byte
            break if number.zero?
          end
          bytes.pack('C*')
        when :string then encode_length_prefixed(value.to_s)
        when :binary then encode_length_prefixed(value.to_s)
        when :string_pair
          key, val = value
          encode_length_prefixed(key.to_s) + encode_length_prefixed(val.to_s)
        else
          raise "Unknown MQTT 5 property type: #{type}"
        end
      end

      def self.encode_length_prefixed(str)
        str = str.dup.force_encoding('ASCII-8BIT')
        [str.bytesize].pack('n') + str
      end

      def self.parse(buffer)
        properties = {}
        until buffer.empty?
          id = buffer.slice!(0...1).unpack1('C')
          name, type = BY_ID.fetch(id) do
            raise ProtocolException, "Unsupported MQTT 5 property identifier: 0x%02X" % id
          end
          value = parse_value(buffer, type)
          store(properties, name, value)
        end
        properties
      end

      def self.parse_value(buffer, type)
        case type
        when :byte then buffer.slice!(0...1).unpack1('C')
        when :short then buffer.slice!(0..1).unpack1('n')
        when :int then buffer.slice!(0..3).unpack1('N')
        when :variable_int then parse_variable_int(buffer)
        when :string, :binary then parse_length_prefixed(buffer)
        when :string_pair
          key = parse_length_prefixed(buffer)
          value = parse_length_prefixed(buffer)
          [key, value]
        else
          raise "Unknown MQTT 5 property type: #{type}"
        end
      end

      def self.parse_variable_int(buffer)
        multiplier = 1
        value = 0
        4.times do |pos|
          byte = buffer.slice!(0...1).unpack1('C')
          value += (byte & 0x7F) * multiplier
          return value if (byte & 0x80).zero?
          multiplier *= 128
          raise ProtocolException, 'Malformed variable byte integer' if pos == 3
        end
      end

      def self.parse_length_prefixed(buffer)
        length = buffer.slice!(0..1).unpack1('n')
        buffer.slice!(0...length)
      end

      def self.store(properties, name, value)
        if MULTI_VALUED.include?(name)
          properties[name] = Array(properties[name]) + [value]
        else
          properties[name] = value
        end
      end
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

      # Hash of MQTT 5 properties (only used with protocol version 5.0)
      attr_accessor :properties

      # Default attribute values
      ATTR_DEFAULTS = {
        :topic => nil,
        :payload => '',
        :properties => {}
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
        if mqtt5?
          @properties = shift_properties(buffer)
        else
          @properties = {}
        end
        @payload = buffer
      end

      # MQTT 5 Subscription Identifiers associated with the message.
      # The server may include one or more identifiers, one for each
      # matching logical subscription.
      def subscription_identifiers
        Array(@properties && @properties[:subscription_identifier])
      end

      def subscription_identifiers=(values)
        @properties ||= {}
        @properties[:subscription_identifier] = Array(values)
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
          "#{inspect_payload}" \
          "#{mqtt5? && !@properties.empty? ? ", #{@properties.inspect}" : ''}>"
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

      # The username for authenticating with the server
      attr_accessor :username

      # The password for authenticating with the server
      attr_accessor :password

      # Default attribute values
      # Hash of MQTT 5 connect properties
      attr_accessor :properties

      # Hash of MQTT 5 will properties
      attr_accessor :will_properties

      ATTR_DEFAULTS = {
        :client_id => nil,
        :clean_session => true,
        :keep_alive => 15,
        :will_topic => nil,
        :will_qos => 0,
        :will_retain => false,
        :will_payload => '',
        :username => nil,
        :password => nil,
        :properties => {},
        :will_properties => {}
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
        elsif mqtt5?
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
        @connect_flags |= ((@will_qos & 0x03) << 3)
        @connect_flags |= 0x20 if @will_retain
        @connect_flags |= 0x40 unless @password.nil?
        @connect_flags |= 0x80 unless @username.nil?
        body += encode_bytes(@connect_flags)

        body += encode_short(@keep_alive)
        body += encode_properties(@properties) if mqtt5?
        body += encode_string(@client_id)
        unless will_topic.nil?
          body += encode_properties(@will_properties) if mqtt5?
          body += encode_string(@will_topic)
          if mqtt5?
            body += (@will_payload || '').to_s.dup.force_encoding('ASCII-8BIT')
          else
            # The MQTT v3.1 specification says that the payload is a UTF-8 string
            body += encode_string(@will_payload)
          end
        end
        body += encode_string(@username) unless @username.nil?
        if !@password.nil?
          if mqtt5?
            body += @password.to_s.dup.force_encoding('ASCII-8BIT')
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
        if mqtt5?
          @properties = shift_properties(buffer)
        else
          @properties = {}
        end
        @client_id = shift_string(buffer)
        if ((@connect_flags & 0x04) >> 2) == 0x01
          # Last Will and Testament
          @will_qos = ((@connect_flags & 0x18) >> 3)
          @will_retain = ((@connect_flags & 0x20) >> 5) == 0x01
          @will_properties = shift_properties(buffer) if mqtt5?
          @will_topic = shift_string(buffer)
          if mqtt5?
            @will_payload = buffer
            buffer = ''.dup
          else
            # The MQTT v3.1 specification says that the payload is a UTF-8 string
            @will_payload = shift_string(buffer)
          end
        end
        if ((@connect_flags & 0x80) >> 7) == 0x01 && buffer.bytesize > 0
          @username = shift_string(buffer)
        end
        if ((@connect_flags & 0x40) >> 6) == 0x01 && buffer.bytesize > 0
          @password = mqtt5? ? buffer.dup : shift_string(buffer)
          buffer.clear if mqtt5?
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

      # Hash of MQTT 5 properties
      attr_accessor :properties

      # Default attribute values
      ATTR_DEFAULTS = { :return_code => 0x00, :properties => {} }

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

        if mqtt5?
          @properties = shift_properties(buffer)
        else
          @properties = {}
          return if buffer.empty?
          raise ProtocolException, 'Extra bytes at end of Connect Acknowledgment packet'
        end
      end

      # Returns a human readable string, summarising the properties of the packet
      def inspect
        "\#<#{self.class}: 0x%2.2X>" % return_code
      end
    end

    # Class representing an MQTT Publish Acknowledgment packet
    class Puback < MQTT::Packet
      # MQTT 5 reason code (defaults to 0x00 - success)
      attr_accessor :reason_code

      # Hash of MQTT 5 properties
      attr_accessor :properties

      ATTR_DEFAULTS = { :reason_code => 0x00, :properties => {} }

      def initialize(args = {})
        super(ATTR_DEFAULTS.merge(args))
      end

      # Get serialisation of packet's body
      def encode_body
        body = encode_short(@id)
        return body unless mqtt5?
        body += encode_bytes(@reason_code.to_i)
        body += encode_properties(@properties)
        body
      end

      # Parse the body (variable header and payload) of a packet
      def parse_body(buffer)
        super(buffer)
        @id = shift_short(buffer)
        if mqtt5?
          @reason_code = buffer.empty? ? 0x00 : shift_byte(buffer)
          @properties = buffer.empty? ? {} : shift_properties(buffer)
        else
          @reason_code = nil
          @properties = {}
          return if buffer.empty?
          raise ProtocolException, 'Extra bytes at end of Publish Acknowledgment packet'
        end
      end

      # Returns a human readable string, summarising the properties of the packet
      def inspect
        "\#<#{self.class}: 0x%2.2X%s>" % [id, mqtt5? ? ", rc=0x%2.2X" % reason_code : '']
      end
    end

    # Class representing an MQTT Publish Received packet
    class Pubrec < MQTT::Packet
      # MQTT 5 reason code (defaults to 0x00 - success)
      attr_accessor :reason_code

      # Hash of MQTT 5 properties
      attr_accessor :properties

      ATTR_DEFAULTS = { :reason_code => 0x00, :properties => {} }

      def initialize(args = {})
        super(ATTR_DEFAULTS.merge(args))
      end

      # Get serialisation of packet's body
      def encode_body
        body = encode_short(@id)
        return body unless mqtt5?
        body += encode_bytes(@reason_code.to_i)
        body += encode_properties(@properties)
        body
      end

      # Parse the body (variable header and payload) of a packet
      def parse_body(buffer)
        super(buffer)
        @id = shift_short(buffer)
        if mqtt5?
          @reason_code = buffer.empty? ? 0x00 : shift_byte(buffer)
          @properties = buffer.empty? ? {} : shift_properties(buffer)
        else
          @reason_code = nil
          @properties = {}
          return if buffer.empty?
          raise ProtocolException, 'Extra bytes at end of Publish Received packet'
        end
      end

      # Returns a human readable string, summarising the properties of the packet
      def inspect
        "\#<#{self.class}: 0x%2.2X%s>" % [id, mqtt5? ? ", rc=0x%2.2X" % reason_code : '']
      end
    end

    # Class representing an MQTT Publish Release packet
    class Pubrel < MQTT::Packet
      # Default attribute values
      ATTR_DEFAULTS = {
        :flags => [false, true, false, false],
        :reason_code => 0x00,
        :properties => {}
      }

      # MQTT 5 reason code
      attr_accessor :reason_code

      # Hash of MQTT 5 properties
      attr_accessor :properties

      # Create a new Pubrel packet
      def initialize(args = {})
        super(ATTR_DEFAULTS.merge(args))
      end

      # Get serialisation of packet's body
      def encode_body
        body = encode_short(@id)
        return body unless mqtt5?
        body += encode_bytes(@reason_code.to_i)
        body += encode_properties(@properties)
        body
      end

      # Parse the body (variable header and payload) of a packet
      def parse_body(buffer)
        super(buffer)
        @id = shift_short(buffer)
        if mqtt5?
          @reason_code = buffer.empty? ? 0x00 : shift_byte(buffer)
          @properties = buffer.empty? ? {} : shift_properties(buffer)
        else
          @reason_code = nil
          @properties = {}
          return if buffer.empty?
          raise ProtocolException, 'Extra bytes at end of Publish Release packet'
        end
      end

      # Check that fixed header flags are valid for this packet type
      # @private
      def validate_flags
        return if @flags == [false, true, false, false]
        raise ProtocolException, 'Invalid flags in PUBREL packet header'
      end

      # Returns a human readable string, summarising the properties of the packet
      def inspect
        "\#<#{self.class}: 0x%2.2X%s>" % [id, mqtt5? ? ", rc=0x%2.2X" % reason_code : '']
      end
    end

    # Class representing an MQTT Publish Complete packet
    class Pubcomp < MQTT::Packet
      # MQTT 5 reason code
      attr_accessor :reason_code

      # Hash of MQTT 5 properties
      attr_accessor :properties

      ATTR_DEFAULTS = { :reason_code => 0x00, :properties => {} }

      def initialize(args = {})
        super(ATTR_DEFAULTS.merge(args))
      end

      # Get serialisation of packet's body
      def encode_body
        body = encode_short(@id)
        return body unless mqtt5?
        body += encode_bytes(@reason_code.to_i)
        body += encode_properties(@properties)
        body
      end

      # Parse the body (variable header and payload) of a packet
      def parse_body(buffer)
        super(buffer)
        @id = shift_short(buffer)
        if mqtt5?
          @reason_code = buffer.empty? ? 0x00 : shift_byte(buffer)
          @properties = buffer.empty? ? {} : shift_properties(buffer)
        else
          @reason_code = nil
          @properties = {}
          return if buffer.empty?
          raise ProtocolException, 'Extra bytes at end of Publish Complete packet'
        end
      end

      # Returns a human readable string, summarising the properties of the packet
      def inspect
        "\#<#{self.class}: 0x%2.2X%s>" % [id, mqtt5? ? ", rc=0x%2.2X" % reason_code : '']
      end
    end

    # Class representing an MQTT Client Subscribe packet
    class Subscribe < MQTT::Packet
      # One or more topic filters to subscribe to
      attr_accessor :topics

      # Per-filter MQTT 5 subscription options, in the same order as {#topics}
      attr_reader :topic_options

      # Hash of MQTT 5 properties
      attr_accessor :properties

      # Default attribute values
      ATTR_DEFAULTS = {
        :topics => [],
        :flags => [false, true, false, false],
        :properties => {}
      }

      # Create a new Subscribe packet
      def initialize(args = {})
        super(ATTR_DEFAULTS.merge(args))
        @topic_options = [] if @topic_options.nil?
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
        @topic_options = []
        until input.empty?
          item = input.shift
          if item.is_a?(Hash)
            # Convert hash into an ordered array of arrays
            item.each do |filter, options|
              options = options.is_a?(Hash) ? options : { :qos => options || 0 }
              @topics << [filter, options_qos_byte(options)]
              @topic_options << options
            end
          elsif item.is_a?(String)
            # Peek at the next item in the array, and remove it if it is an integer
            if input.first.is_a?(Integer)
              qos = input.shift
              @topics << [item, qos]
              @topic_options << { :qos => qos }
            elsif input.first.is_a?(Hash)
              options = input.shift
              @topics << [item, options_qos_byte(options)]
              @topic_options << options
            else
              @topics << [item, 0]
              @topic_options << { :qos => 0 }
            end
          else
            # Meh?
            raise "Invalid topics input: #{value.inspect}"
          end
        end
        @topics
      end

      # The MQTT 5 Subscription Identifier of the SUBSCRIBE packet (if any)
      def subscription_identifier
        values = Array(@properties[:subscription_identifier])
        values.first
      end

      def subscription_identifier=(value)
        if value.nil?
          @properties.delete(:subscription_identifier)
        else
          validate_subscription_identifier(value)
          @properties[:subscription_identifier] = value
        end
      end

      # Return just the topic filters, in subscription order
      def topic_filters
        @topics.map(&:first)
      end

      # Return the granted QoS values in subscription order
      def requested_qos_levels
        @topics.map { |_filter, options_byte| options_byte & 0x03 }
      end

      # Get serialisation of packet's body
      def encode_body
        raise 'no topics given when serialising packet' if @topics.empty?
        body = encode_short(@id)
        body += encode_properties(@properties) if mqtt5?
        @topics.each_with_index do |item, index|
          body += encode_string(item[0])
          options = @topic_options[index] || { :qos => item[1] }
          options = options.merge(:qos => item[1] & 0x03)
          options_byte = mqtt5? ? encode_filter_options(options) : item[1]
          body += encode_bytes(options_byte)
        end
        body
      end

      # Parse the body (variable header and payload) of a packet
      def parse_body(buffer)
        super(buffer)
        @id = shift_short(buffer)
        @properties = mqtt5? ? shift_properties(buffer) : {}
        @topics = []
        while buffer.bytesize > 0
          topic_name = shift_string(buffer)
          topic_qos = shift_byte(buffer)
          @topics << [topic_name, mqtt5? ? topic_qos & 0x03 : topic_qos]
          @topic_options << { :qos => topic_qos & 0x03, :raw_options => topic_qos }
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
          topics.map { |t| "'#{t[0]}':#{t[1] & 0x03}" }.join(', ')
        ]
      end

      private

      def options_qos_byte(options)
        options[:qos] ||= 0
        options[:qos] & 0x03
      end

      def encode_filter_options(options)
        byte = (options[:qos] & 0x03)
        byte |= 0x04 if options[:no_local]
        byte |= 0x08 if options[:retain_as_published]
        retain_handling = (options[:retain_handling] || 0) & 0x03
        byte | (retain_handling << 4)
      end

      def validate_subscription_identifier(value)
        number = value.to_i
        return if number >= 1 && number <= 268_435_455
        raise ArgumentError, 'Subscription identifier must be between 1 and 268435455'
      end
    end

    # Class representing an MQTT Subscribe Acknowledgment packet
    class Suback < MQTT::Packet
      # An array of return codes, ordered by the topics that were subscribed to
      attr_accessor :return_codes

      # Hash of MQTT 5 properties
      attr_accessor :properties

      # Default attribute values
      ATTR_DEFAULTS = {
        :return_codes => [],
        :properties => {}
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
        @properties = mqtt5? ? shift_properties(buffer) : {}
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

      # Hash of MQTT 5 properties
      attr_accessor :properties

      # Default attribute values
      ATTR_DEFAULTS = {
        :topics => [],
        :flags => [false, true, false, false],
        :properties => {}
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
        @properties = mqtt5? ? shift_properties(buffer) : {}
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
      # An array of MQTT 5 reason codes, ordered by the filters that were
      # unsubscribed from
      attr_accessor :reason_codes

      # Hash of MQTT 5 properties
      attr_accessor :properties

      # Create a new Unsubscribe Acknowledgment packet
      ATTR_DEFAULTS = { :reason_codes => nil, :properties => {} }

      def initialize(args = {})
        super(ATTR_DEFAULTS.merge(args))
      end

      def reason_codes=(value)
        @reason_codes = value.nil? ? [] : Array(value)
      end

      # Get serialisation of packet's body
      def encode_body
        body = encode_short(@id)
        return body unless mqtt5?
        body += encode_properties(@properties)
        body += encode_bytes(*@reason_codes) unless @reason_codes.nil? || @reason_codes.empty?
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
        else
          @properties = {}
          @reason_codes = nil
          return if buffer.empty?
          raise ProtocolException, 'Extra bytes at end of Unsubscribe Acknowledgment packet'
        end
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
      # MQTT 5 reason code
      attr_accessor :reason_code

      # Hash of MQTT 5 properties
      attr_accessor :properties

      ATTR_DEFAULTS = { :reason_code => 0x00, :properties => {} }

      # Create a new Ping Response packet
      def initialize(args = {})
        super(ATTR_DEFAULTS.merge(args))
      end

      # Check the body
      def parse_body(buffer)
        super(buffer)

        if mqtt5?
          @reason_code = buffer.empty? ? 0x00 : shift_byte(buffer)
          @properties = buffer.empty? ? {} : shift_properties(buffer)
        else
          return if buffer.empty?
          raise ProtocolException, 'Extra bytes at end of Ping Response packet'
        end
      end
    end

    # Class representing an MQTT Client Disconnect packet
    class Disconnect < MQTT::Packet
      # MQTT 5 reason code
      attr_accessor :reason_code

      # Hash of MQTT 5 properties
      attr_accessor :properties

      ATTR_DEFAULTS = { :reason_code => 0x00, :properties => {} }

      # Create a new Client Disconnect packet
      def initialize(args = {})
        super(ATTR_DEFAULTS.merge(args))
      end

      # Get serialisation of packet's body
      def encode_body
        return '' unless mqtt5?
        encode_bytes(@reason_code.to_i) + encode_properties(@properties)
      end

      # Check the body
      def parse_body(buffer)
        super(buffer)

        if mqtt5?
          @reason_code = buffer.empty? ? 0x00 : shift_byte(buffer)
          @properties = buffer.empty? ? {} : shift_properties(buffer)
        else
          return if buffer.empty?
          raise ProtocolException, 'Extra bytes at end of Disconnect packet'
        end
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
    nil
  ]
end

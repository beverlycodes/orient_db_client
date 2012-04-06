require 'orient_db_client/rid'

require 'base64'

module OrientDbClient
    module Serializers
        class Serializer7
            @@string_matcher = /^"[^"]*"$/

            def serialize(record)
                unless record[:document]
                    record = { :document => record }
                end

                serialize_document(record)
            end

            private

            def binary_encoding
                @binary_encoding = @binary_encoding || Encoding.find("ASCII-8BIT")
            end

            def serialize_document(record, embedded = false)
                structure = record[:structure] || {}
                result = []
                recordClass = ''
                document = nil

                if record[:document]
                    recordClass = record[:class] ? "#{record[:class]}@" : ""
                    document = record[:document]
                else
                    document = record
                end

                document.each do |k, v|
                    key_struct = structure[k.to_s] || structure[k.to_sym]
                    result << "#{k}:#{serialize_item(v, key_struct)}"
                end

                serialized_document = "#{recordClass}#{result.join(",")}"

                serialized_document = "(#{serialized_document})" if embedded

                serialized_document
            end

            def serialize_array(value)
                result = []

                value.each do |item|
                    result << serialize_item(item)
                end

                "[#{result.join(",")}]"
            end

            def serialize_binary(value)
                "_#{Base64.encode64(value).chomp}_"
            end

            def serialize_boolean(value)
                value ? "true" : "false"
            end

            def serialize_byte(value)
                "#{value.ord}b"
            end

            def serialize_date(value)
                if value.is_a?(Time)
                    value = Date.new(value.year, value.month, value.day)
                end

                value = Date.parse(value) unless value.is_a?(Date)

                "#{value.to_time.to_i}a"
            end

            def serialize_decimal(value)
                "#{value}c"
            end

            def serialize_double(value)
                "#{value}d"
            end

            def serialize_float(value)
                "#{value}f"
            end

            def serialize_item(value, type = nil)
                case type
                when :binary
                    serialize_binary(value)
                when :boolean
                    serialize_boolean(value)
                when :byte
                    serialize_byte(value)
                when :collection
                    serialize_array(value)
                when :date
                    serialize_date(value)
                when :decimal
                    serialize_decimal(value)
                when :document
                    serialize_document(value, true)
                when :double
                    serialize_double(value)
                when :float
                    serialize_float(value)
                when :integer
                    serialize_integer(value)
                when :long
                    serialize_long(value)
                when :map
                    serialize_map(value)
                when :rid
                    serialize_rid(value)
                when :short
                    serialize_short(value)
                when :string
                    serialize_string(value)
                when :time
                    serialize_time(value)
                else
                    serialize_unknown(value)
                end
            end

            def serialize_integer(value)
                value.to_s
            end

            def serialize_long(value)
                "#{value}l"
            end

            def serialize_map(value)
                result = []
                value.each do |k, v|
                    result << "\"#{k.to_s}\":#{serialize_unknown(v)}"
                end

                "{#{result.join(",")}}"
            end

            def serialize_nil
                ''
            end

            def serialize_rid(value)
                value = OrientDbClient::Rid.new(value.to_s) unless value.is_a?(OrientDbClient::Rid)

                "#{value.to_s}"
            end

            def serialize_short(value)
                "#{value}s"
            end

            def serialize_string(value)
                "\"#{value.to_s}\""
            end

            def serialize_time(value)
                value = DateTime.parse(value) if value.is_a?(String)
                value = value.to_time if value.is_a?(DateTime)

                "#{value.to_i}t"
            end

            def serialize_unknown(value)
                if value.is_a?(OrientDbClient::Rid)
                    serialize_rid(value)
                elsif value.is_a?(String)
                    serialize_string(value)
                elsif value.is_a?(Fixnum)
                    serialize_integer(value)
                elsif value.is_a?(TrueClass)
                    serialize_boolean(value)
                elsif value.is_a?(FalseClass)
                    serialize_boolean(value)
                elsif value.is_a?(Float)
                    serialize_double(value)
                elsif value.is_a?(Bignum)
                    serialize_long(value)
                elsif value.is_a?(Array)
                    serialize_array(value)
                elsif value.is_a?(Time)
                    serialize_time(value)
                elsif value.is_a?(Date)
                    serialize_date(value)
                elsif value.is_a?(Hash)
                    if value[:document]
                        serialize_document(value, true)
                    else
                        serialize_map(value)
                    end
                elsif value.nil?
                    serialize_nil
                end
            end
        end
    end
end
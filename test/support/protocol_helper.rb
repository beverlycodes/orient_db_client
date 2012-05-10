require 'bindata'

module ProtocolHelper
  def pack_byte(value)
    if value.is_a?(String)
      value = value.length > 0 ? value[0].ord : 0
    end

    BinData::Int8.new(value).to_binary_s
  end

  def pack_integer(value)
    BinData::Int32be.new(value).to_binary_s
  end

  def pack_long(value)
    BinData::Int64be.new(value).to_binary_s
  end

  def pack_short(value)
    BinData::Int16be.new(value).to_binary_s
  end

  def pack_string(value)
    [ value ].pack('a*')
  end
end

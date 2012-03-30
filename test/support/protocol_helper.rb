module ProtocolHelper
  def pack_byte(value)
    [ value ].pack('C')
  end

  def pack_integer(value)
    [ value ].pack('l>')
  end

  def pack_long(value)
    [ value ].pack('q>')
  end

  def pack_short(value)
    [ value ].pack('s>')
  end

  def pack_string(value)
    [ value ].pack('a*')
  end
end

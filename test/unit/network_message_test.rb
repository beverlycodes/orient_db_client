require File.join File.dirname(__FILE__), '..', 'test_helper'

class TestNetworkMessage < MiniTest::Unit::TestCase
	def test_pack
		test_string = "This string is automatically preceeded by its size"

		data = [
			[ :byte, 143 ],
			[ :short, 5000 ],
			[ :integer, 153000 ],
			[ :raw_string, "This is a string" ],
			[ :integer, test_string.length, :skip ],
			[ :string, test_string ]
		]

		expected = data.map { |d| d[1] }.pack('C s> l> a* l> a*')

		message = OrientDbClient::NetworkMessage.new { |m|
			data.each { |d|	m.add d[0], d[1] unless d[2] == :skip }
		}

		assert_equal expected, message.pack
	end
end
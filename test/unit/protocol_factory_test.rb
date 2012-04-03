require File.join File.dirname(__FILE__), '..', 'test_helper'

require 'orient_db_client/protocol_factory'

class TestProtocolFactory < MiniTest::Unit::TestCase
	def test_returns_protocol7_instance
 	 	assert_equal OrientDbClient::Protocols::Protocol7, OrientDbClient::ProtocolFactory.get_protocol(7)
 	end

    def test_returns_protocol9_instance
        assert_equal OrientDbClient::Protocols::Protocol9, OrientDbClient::ProtocolFactory.get_protocol(9)
    end
end


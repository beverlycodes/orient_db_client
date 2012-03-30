require File.join File.dirname(__FILE__), '..', 'test_helper'

class TestConnection < MiniTest::Unit::TestCase
	include ServerConfig
	include ConnectionHelper

	def setup
		@connection = connect_to_orientdb(SERVER_OPTIONS)
	end

	def teardown
		@connection.close if @connection
	end

  def test_establishing_a_connection
    assert_instance_of OrientDbClient::Connection, @connection
  end
end

require File.join File.dirname(__FILE__), '..', 'test_helper'

class TestOpenServer < MiniTest::Unit::TestCase
	include ServerConfig
	include ConnectionHelper

	def setup
		@options = SERVER_OPTIONS
		@connection = connect_to_orientdb(SERVER_OPTIONS)
	end

	def teardown
		@connection.close if @connection
	end

  def test_connect_command
    session = @connection.open_server({
    	:user => @options["server_user"],
    	:password => @options["server_password"]
    })

    refute_nil session.id
  end
end

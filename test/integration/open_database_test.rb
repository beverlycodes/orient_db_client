require File.join File.dirname(__FILE__), '..', 'test_helper'

class TestOpenDatabase < MiniTest::Unit::TestCase
	include ServerConfig
	include ConnectionHelper

	def setup
		@options = SERVER_OPTIONS
		@connection = connect_to_orientdb(@options)
	end

	def teardown
		@connection.close if @connection
	end

  def test_open_database
    session = @connection.open_database(@options["database"], {
    	:user => @options["user"],
    	:password => @options["password"]
    })

    refute_nil session.id

    refute_nil session.get_cluster("orids")
    refute_nil session.get_cluster("ouser")
    refute_nil session.get_cluster("orole")
  end
end

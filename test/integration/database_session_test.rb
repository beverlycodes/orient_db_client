require File.join File.dirname(__FILE__), '..', 'test_helper'

class TestDatabaseSession < MiniTest::Unit::TestCase
	include ServerConfig
	include ConnectionHelper

	def setup
		@options = SERVER_OPTIONS
		@connection = connect_to_orientdb(SERVER_OPTIONS)
		@session = @connection.open_database(@options["database"], {
			:user => @options["user"],
			:password => @options["password"]
		})
	end

	def teardown
		@connection.close if @connection
	end

  # The protocol documentation for DB_CLOSE is very ambiguous.
  # As such, this test doesn't really do anything that makes sense...
  def test_close
    @session.close

    refute @connection.closed?
  end

end

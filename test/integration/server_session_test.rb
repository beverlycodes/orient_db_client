require File.join File.dirname(__FILE__), '..', 'test_helper'

class TestServerSession < MiniTest::Unit::TestCase
	include ServerConfig
	include ConnectionHelper

	def setup
		@options = SERVER_OPTIONS
		@connection = connect_to_orientdb(SERVER_OPTIONS)
		@session = @connection.open_server({
			:user => @options["server_user"],
			:password => @options["server_password"]
		})
	end

	def teardown
		@connection.close if @connection
	end
	
	

  def test_database_exists_command
  	assert @session.database_exists?(@options["database"])
  	refute @session.database_exists?("InvalidDatabase")
  end
  
  def test_config_get
      retval = @session.config_get("network.retry")
      assert retval[:value]
  end

  def test_create_and_delete_local_database_commands
  	database = "test_create_local_database"

    @session.delete_database(database) if @session.database_exists?(database)

  	begin
  		@session.create_local_database(database)
  		assert @session.database_exists?(database)
  	ensure
  		@session.delete_database(database) if @session.database_exists?(database)
  	end
  end

  def test_create_and_delete_memory_database_commands
    skip # Bug in OrientDB-1.0rc9 prevents this test from passing

    database = "test_create_memory_database"

    @session.delete_database(database) if @session.database_exists?(database)

    begin
      @session.create_memory_database(database)
      assert @session.database_exists?(database)
    ensure
      @session.delete_database(database) if @session.database_exists?(database)
    end
  end
end

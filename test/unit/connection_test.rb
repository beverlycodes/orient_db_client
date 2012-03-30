require File.join File.dirname(__FILE__), '..', 'test_helper'

class TestConnection < MiniTest::Unit::TestCase
	include ConnectionHelper
	include ServerConfig

	def setup
		@connection = mock_connect_to_orientdb(7, @socket)
		@options = SERVER_OPTIONS
    @session = 25234
	end

  def test_opening_the_server
    OrientDbClient::Protocols::Protocol7.stubs(:connect).returns({
      :session => @session,
      :message_content => { :session => @session }
    })

    session = @connection.open_server({
      :user => @options["server_user"],
      :password => @options["server_password"]
    })

    assert_instance_of OrientDbClient::ServerSession, session
    assert_equal @session, session.id
  end

  def test_opening_a_database
    OrientDbClient::Protocols::Protocol7.stubs(:db_open).returns({
      :session => @session,
      :message_content => { :session => @session, :clusters => [] }
    })

  	session = @connection.open_database(@options["database"], {
  		:user => @options["user"],
  		:password => @options["password"]
  	})

  	assert_instance_of OrientDbClient::DatabaseSession, session
    assert_equal @session, session.id
  end
end
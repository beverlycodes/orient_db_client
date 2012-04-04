require File.join File.dirname(__FILE__), '..', 'test_helper'

class TestConnection < MiniTest::Unit::TestCase
	include ConnectionHelper
	include ServerConfig

	def setup
		@connection = mock_connect_to_orientdb(7, @socket)
		@options = SERVER_OPTIONS
    @session = 25234
	end

  def test_creating_a_cluster
    expected_cluster_id = 65

    OrientDbClient::Protocols::Protocol7.stubs(:datacluster_add).returns({
      :session => @session,
      :message_content => { :new_cluster_number => expected_cluster_id }
    })

    cluster_id = @connection.create_cluster(@session, :physical, "TEST")

    assert_equal expected_cluster_id, cluster_id
  end

  def test_creating_a_record
    cluster_id = 5
    expected_cluster_position = 735

    OrientDbClient::Protocols::Protocol7.stubs(:record_create).returns({
      :session => @session,
      :message_content => { :cluster_id => cluster_id,
                            :cluster_position => expected_cluster_position }
    })

    rid = @connection.create_record(@session, cluster_id, {})

    assert_equal cluster_id, rid.cluster_id
    assert_equal expected_cluster_position, rid.cluster_position
  end

  def test_updating_a_record
    expected_version = 1

    OrientDbClient::Protocols::Protocol7.stubs(:record_update).returns({
      :session => @session,
      :message_content => { :record_version => expected_version }
    })

    version = @connection.update_record(@session, OrientDbClient::Rid.new(1,0), {}, :none)

    assert_equal expected_version, version
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
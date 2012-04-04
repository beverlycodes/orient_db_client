require File.join File.dirname(__FILE__), '..', '..', 'test_helper'

class TestProtocol9 < MiniTest::Unit::TestCase
  include ConnectionHelper
  include ExpectationHelper
  include ProtocolHelper

  SOCKET_RECV_EXPECTATION = lambda do |r|
    expectation = { :method => :read }

    if r.is_a?(Hash)
      expectation[:param] = r[:param] if r[:param]
      expectation[:return] = r[:return] if r[:return]
    else
      expectation[:return] = r
    end

    expectation
  end

  module Sizes
    BYTE    = 1
    SHORT   = 2
    INTEGER = 4
    LONG    = 8
  end

  def setup
    @protocol = OrientDbClient::Protocols::Protocol9
    @protocol_version = @protocol::VERSION
    @session = 62346
    @database = 'test_database'
    @user = 'test_user'
    @password = 'test_password'

    @driver_name = @protocol::DRIVER_NAME
    @driver_version = @protocol::DRIVER_VERSION

    @clusters = [
      { :name => 'vertexes',
        :id => 0,
        :type => 'PHYSICAL' },

      { :name => "edges",
        :id => 1,
        :type => 'LOGICAL' } ]

    @socket = mock()
  end

  def socket_receives(request)
    @socket.expects(:write).with(request).returns(nil)
  end

  def test_command
    command_string = 'SELECT FROM OUser'

    command = OrientDbClient::NetworkMessage.new { |m|
      m.add :string,  'q'
      m.add :string,  command_string
      m.add :integer, -1
      m.add :integer, 0
    }.pack

    request = OrientDbClient::NetworkMessage.new { |m| 
      m.add :byte,    @protocol::Operations::COMMAND
      m.add :integer, @session
      m.add :byte,    's'
      m.add :string,  command
    }.pack

    socket_receives(request)

    chain = [
      pack_byte(@protocol::Statuses::OK),
      pack_integer(@session),
      pack_byte(@protocol::PayloadStatuses::NULL),
      pack_byte(@protocol::PayloadStatuses::NO_RECORDS)
    ].map! &SOCKET_RECV_EXPECTATION

    expect_sequence @socket, chain, 'response'

    result = @protocol.command(@socket, @session, command_string)

    assert_equal @session, result[:session]

    assert result[:message_content].is_a?(Array)
    assert_equal 1, result[:message_content].length
    assert_nil  result[:message_content][0]
  end

  def test_db_create
    storage_type = 'local'
    database_type = 'document'

    request = OrientDbClient::NetworkMessage.new { |m|
      m.add :byte,    @protocol::Operations::DB_CREATE
      m.add :integer, @session
      m.add :string,  @database
      m.add :string,  database_type
      m.add :string,  storage_type
    }.pack

    socket_receives(request)

    chain = [
      pack_byte(@protocol::Statuses::OK),
      pack_integer(@session)
    ].map! &SOCKET_RECV_EXPECTATION

    expect_sequence @socket, chain, 'response'

    result = @protocol.db_create(@socket, @session, @database, {
      :database_type => database_type,
      :storage_type => storage_type
    })

    assert_equal @session, result[:session]
  end

  def test_record_load
    cluster_id = 3
    cluster_position = 6

    request = OrientDbClient::NetworkMessage.new { |m|
      m.add :byte,    @protocol::Operations::RECORD_LOAD
      m.add :integer, @session
      m.add :short,   cluster_id
      m.add :long,    cluster_position
      m.add :string,  ""
      m.add :byte,    0
    }.pack

    socket_receives(request)

    chain = [
      pack_byte(@protocol::Statuses::OK),
      pack_integer(@session),
      pack_byte(@protocol::PayloadStatuses::NO_RECORDS)
    ].map! &SOCKET_RECV_EXPECTATION

    expect_sequence @socket, chain, 'response'

    result = @protocol.record_load(@socket, @session, OrientDbClient::Rid.new(cluster_id, cluster_position))

    assert_equal @session, result[:session]
  end

end
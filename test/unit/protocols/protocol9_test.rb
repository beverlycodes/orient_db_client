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
    @socket.stubs(:pos)
    @socket.stubs(:write)
    @socket.stubs(:read).with(0).returns('')
  end

  def socket_receives(request)
    @socket.expects(:write).with(request).returns(nil)
  end

  def test_command
    command_string = 'SELECT FROM OUser'

    command = @protocol::QueryMessage.new :query_class_name => 'q',
                                          :text => command_string

    inputs = sequence('inputs')
    @socket.expects(:write).with(pack_byte(@protocol::Operations::COMMAND)).in_sequence(inputs)
    @socket.expects(:write).with(pack_integer(@session)).in_sequence(inputs)
    @socket.expects(:write).with(pack_byte('s'.ord)).in_sequence(inputs)
    @socket.expects(:write).with(pack_string(command.to_binary_s)).in_sequence(inputs)

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

    inputs = sequence('inputs')
    @socket.expects(:write).with(pack_byte(@protocol::Operations::DB_CREATE)).in_sequence(inputs)
    @socket.expects(:write).with(pack_integer(@session)).in_sequence(inputs)
    @socket.expects(:write).with(pack_string(@database)).in_sequence(inputs)
    @socket.expects(:write).with(pack_string(database_type)).in_sequence(inputs)
    @socket.expects(:write).with(pack_string(storage_type)).in_sequence(inputs)

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

  def test_db_open
    inputs = sequence('inputs')
    @socket.expects(:write).with(pack_byte(@protocol::Operations::DB_OPEN)).in_sequence(inputs)
    @socket.expects(:write).with(pack_integer(@protocol::NEW_SESSION)).in_sequence(inputs)
    @socket.expects(:write).with(pack_string(@driver_name)).in_sequence(inputs)
    @socket.expects(:write).with(pack_string(@driver_version)).in_sequence(inputs)
    @socket.expects(:write).with(pack_short(@protocol_version)).in_sequence(inputs)
    @socket.expects(:write).with(pack_string(@database)).in_sequence(inputs)
    @socket.expects(:write).with(pack_string('document')).in_sequence(inputs)
    @socket.expects(:write).with(pack_string(@user)).in_sequence(inputs)
    @socket.expects(:write).with(pack_string(@password)).in_sequence(inputs)

    # recv chain
    chain = [
      { :param => Sizes::BYTE,    :return => pack_byte(@protocol::Statuses::OK) },
      { :param => Sizes::INTEGER, :return => pack_integer(@protocol::NEW_SESSION) },
      { :param => Sizes::INTEGER, :return => pack_integer(@session) },
      { :param => Sizes::SHORT,   :return => pack_short(@clusters.length) }
    ]

    @clusters.each do |cluster|
      chain.concat [
        { :param => Sizes::INTEGER,         :return => pack_integer(cluster[:name].length) },
        { :param => cluster[:name].length,  :return => pack_string(cluster[:name]) },
        { :param => Sizes::SHORT,           :return => pack_short(cluster[:id]) },
        { :param => Sizes::INTEGER,         :return => pack_integer(cluster[:type].length) },
        { :param => cluster[:type].length,  :return => pack_string(cluster[:type]) }
      ]
    end

    chain << { :param => Sizes::INTEGER,    :return => pack_integer(0) }

    chain.map! &SOCKET_RECV_EXPECTATION
    # End recv chain

    expect_sequence @socket, chain, 'response'

    result = @protocol.db_open(@socket, @database, {
      :user => @user,
      :password => @password
    })

    assert_equal @protocol::NEW_SESSION, result[:session]
    assert_equal @session, result[:message_content][:session]
    assert_equal @clusters.length, result[:message_content][:clusters].length

    @clusters.each_with_index do |c, i|
      assert_equal @clusters[i][:id], result[:message_content][:clusters][i][:id]
      assert_equal @clusters[i][:name], result[:message_content][:clusters][i][:name]
      assert_equal @clusters[i][:type], result[:message_content][:clusters][i][:type]
    end
  end

  def test_record_load
    cluster_id = 3
    cluster_position = 6

    inputs = sequence('inputs')
    @socket.expects(:write).with(pack_byte(@protocol::Operations::RECORD_LOAD)).in_sequence(inputs)
    @socket.expects(:write).with(pack_integer(@session)).in_sequence(inputs)
    @socket.expects(:write).with(pack_short(cluster_id)).in_sequence(inputs)
    @socket.expects(:write).with(pack_long(cluster_position)).in_sequence(inputs)
    @socket.expects(:write).with(pack_integer(0)).in_sequence(inputs)
    @socket.expects(:write).with(pack_byte(0)).in_sequence(inputs)

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
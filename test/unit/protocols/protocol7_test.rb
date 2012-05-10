require File.join File.dirname(__FILE__), '..', '..', 'test_helper'

class TestProtocol7 < MiniTest::Unit::TestCase
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
    @protocol = OrientDbClient::Protocols::Protocol7
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

  def test_connect
    inputs = sequence('inputs')

    @socket.expects(:write).once.with(pack_string(@user)).in_sequence(inputs)
    @socket.expects(:write).once.with(pack_string(@password)).in_sequence(inputs)

    chain = [
      pack_byte(@protocol::Statuses::OK),
      pack_integer(@protocol::NEW_SESSION),
      pack_integer(@session)
    ].map! &SOCKET_RECV_EXPECTATION

    expect_sequence @socket, chain, 'response'
    
    result = @protocol.connect(@socket, {
      :user => @user,
      :password => @password
    })

    assert_equal @protocol::NEW_SESSION, result[:session]
    assert_equal @session, result[:message_content][:session]
  end

  def test_count
    cluster_name = "vertexes"
    record_count = 1564

    inputs = sequence('inputs')

    @socket.expects(:write).once.with(pack_integer(@session)).in_sequence(inputs)
    @socket.expects(:write).once.with(pack_string(cluster_name)).in_sequence(inputs)

    chain = [
      pack_byte(@protocol::Statuses::OK),
      pack_integer(@session),
      pack_long(record_count)
    ].map! &SOCKET_RECV_EXPECTATION

    expect_sequence @socket, chain, 'response'
    
    result = @protocol.count(@socket, @session, cluster_name)

    assert_equal record_count, result[:message_content][:record_count]
  end

  def test_command
    command_string = 'SELECT FROM OUser'

    command = @protocol::QueryMessage.new :query_class_name => 'com.orientechnologies.orient.core.sql.query.OSQLSynchQuery',
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

  def test_datacluster_add_logical
    type = :logical       # Use symbol here, please
    container = 15

    new_cluster_number = 20

    inputs = sequence('inputs')
    @socket.expects(:write).with(pack_byte(@protocol::Operations::DATACLUSTER_ADD)).in_sequence(inputs)
    @socket.expects(:write).with(pack_integer(@session)).in_sequence(inputs)
    @socket.expects(:write).with(pack_string('LOGICAL')).in_sequence(inputs)
    @socket.expects(:write).with(pack_integer(container)).in_sequence(inputs)

    chain = [
      { :param => Sizes::BYTE,    :return => pack_byte(@protocol::Statuses::OK) },
      { :param => Sizes::INTEGER, :return => pack_integer(@session) },
      { :param => Sizes::SHORT,   :return => pack_short(new_cluster_number) }
    ].map! &SOCKET_RECV_EXPECTATION

    expect_sequence @socket, chain, 'response'

    result = @protocol.datacluster_add(@socket, @session, type, {
      :physical_cluster_container_id => container
    })

    assert_equal @session, result[:session]
    assert_equal new_cluster_number, result[:message_content][:new_cluster_number]
  end

  def test_datacluster_add_memory
    type = :memory
    name = 'test_memory_cluster'

    new_cluster_number = 6

    inputs = sequence('inputs')
    @socket.expects(:write).with(pack_byte(@protocol::Operations::DATACLUSTER_ADD)).in_sequence(inputs)
    @socket.expects(:write).with(pack_integer(@session)).in_sequence(inputs)
    @socket.expects(:write).with(pack_string('MEMORY')).in_sequence(inputs)
    @socket.expects(:write).with(pack_string(name)).in_sequence(inputs)

    chain = [
      { :param => Sizes::BYTE,    :return => pack_byte(@protocol::Statuses::OK) },
      { :param => Sizes::INTEGER, :return => pack_integer(@session) },
      { :param => Sizes::SHORT,   :return => pack_short(new_cluster_number) }
    ].map! &SOCKET_RECV_EXPECTATION

    expect_sequence @socket, chain, 'response'

    result = @protocol.datacluster_add(@socket, @session, type, {
      :name => name
    })

    assert_equal @session, result[:session]
    assert_equal new_cluster_number, result[:message_content][:new_cluster_number]
  end

  def test_datacluster_add_physical
    type = 'PHYSICAL'       # Use string here, please
    name = 'test_cluster'
    file_name = 'test_cluster_file'
    size = -1

    new_cluster_number = 10

    inputs = sequence('inputs')
    @socket.expects(:write).with(pack_byte(@protocol::Operations::DATACLUSTER_ADD)).in_sequence(inputs)
    @socket.expects(:write).with(pack_integer(@session)).in_sequence(inputs)
    @socket.expects(:write).with(pack_string('PHYSICAL')).in_sequence(inputs)
    @socket.expects(:write).with(pack_string(name)).in_sequence(inputs)
    @socket.expects(:write).with(pack_string(file_name)).in_sequence(inputs)
    @socket.expects(:write).with(pack_integer(size)).in_sequence(inputs)

    chain = [
      { :param => Sizes::BYTE,    :return => pack_byte(@protocol::Statuses::OK) },
      { :param => Sizes::INTEGER, :return => pack_integer(@session) },
      { :param => Sizes::SHORT,   :return => pack_short(new_cluster_number) }
    ].map! &SOCKET_RECV_EXPECTATION

    expect_sequence @socket, chain, 'response'

    result = @protocol.datacluster_add(@socket, @session, type, {
      :name => name,
      :file_name => file_name,
      :initial_size => size
    })

    assert_equal @session, result[:session]
    assert_equal new_cluster_number, result[:message_content][:new_cluster_number]
  end

  def test_datacluster_datarange
    cluster_id = 1
    range_begin = 0
    range_end = 1000

    inputs = sequence('inputs')
    @socket.expects(:write).with(pack_byte(@protocol::Operations::DATACLUSTER_DATARANGE)).in_sequence(inputs)
    @socket.expects(:write).with(pack_integer(@session)).in_sequence(inputs)
    @socket.expects(:write).with(pack_short(cluster_id)).in_sequence(inputs)

    chain = [
      { :param => Sizes::BYTE,    :return => pack_byte(@protocol::Statuses::OK) },
      { :param => Sizes::INTEGER, :return => pack_integer(@session) },
      { :param => Sizes::LONG,    :return => pack_long(range_begin) },
      { :param => Sizes::LONG,    :return => pack_long(range_end) }
    ].map! &SOCKET_RECV_EXPECTATION

    expect_sequence @socket, chain, 'response'

    result = @protocol.datacluster_datarange(@socket, @session, cluster_id)

    assert_equal @session,    result[:session]
    assert_equal range_begin, result[:message_content][:begin]
    assert_equal range_end,   result[:message_content][:end]
  end

  def test_datacluster_remove
    id = 10

    inputs = sequence('inputs')
    @socket.expects(:write).with(pack_byte(@protocol::Operations::DATACLUSTER_REMOVE)).in_sequence(inputs)
    @socket.expects(:write).with(pack_integer(@session)).in_sequence(inputs)
    @socket.expects(:write).with(pack_short(id)).in_sequence(inputs)

    chain = [
      { :param => Sizes::BYTE,    :return => pack_byte(@protocol::Statuses::OK) },
      { :param => Sizes::INTEGER, :return => pack_integer(@session) },
      { :param => Sizes::BYTE,    :return => pack_byte(1) }
    ].map! &SOCKET_RECV_EXPECTATION

    expect_sequence @socket, chain, 'response'

    result = @protocol.datacluster_remove(@socket, @session, id)

    assert_equal @session, result[:session]
    assert_equal 1, result[:message_content][:result]
  end

  def test_db_close
    inputs = sequence('inputs')
    @socket.expects(:write).with(pack_byte(@protocol::Operations::DB_CLOSE)).in_sequence(inputs)
    @socket.expects(:write).with(pack_integer(@session)).in_sequence(inputs)

    @socket.expects(:closed?).returns(true)

    result = @protocol.db_close(@socket, @session)

    assert result
  end

  def test_db_countrecords
    count = 26345

    inputs = sequence('inputs')
    @socket.expects(:write).with(pack_byte(@protocol::Operations::DB_COUNTRECORDS)).in_sequence(inputs)
    @socket.expects(:write).with(pack_integer(@session)).in_sequence(inputs)

    chain = [
      { :param => Sizes::BYTE,    :return => pack_byte(@protocol::Statuses::OK) },
      { :param => Sizes::INTEGER, :return => pack_integer(@session) },
      { :param => Sizes::LONG,    :return => pack_long(count) }
    ].map! &SOCKET_RECV_EXPECTATION

    expect_sequence @socket, chain, 'response'

    result = @protocol.db_countrecords(@socket, @session)

    assert_equal @session, result[:session]
    assert_equal count, result[:message_content][:count]
  end

  def test_db_create
    storage_type = 'local'

    inputs = sequence('inputs')
    @socket.expects(:write).with(pack_byte(@protocol::Operations::DB_CREATE)).in_sequence(inputs)
    @socket.expects(:write).with(pack_integer(@session)).in_sequence(inputs)
    @socket.expects(:write).with(pack_string(@database)).in_sequence(inputs)
    @socket.expects(:write).with(pack_string(storage_type)).in_sequence(inputs)

    chain = [
      pack_byte(@protocol::Statuses::OK),
      pack_integer(@session)
    ].map! &SOCKET_RECV_EXPECTATION

    expect_sequence @socket, chain, 'response'

    result = @protocol.db_create(@socket, @session, @database, storage_type)

    assert_equal @session, result[:session]
  end

  def test_db_delete
    inputs = sequence('inputs')
    @socket.expects(:write).with(pack_byte(@protocol::Operations::DB_DELETE)).in_sequence(inputs)
    @socket.expects(:write).with(pack_integer(@session)).in_sequence(inputs)
    @socket.expects(:write).with(pack_string(@database)).in_sequence(inputs)

    chain = [
      { :param => Sizes::BYTE,    :return => pack_byte(@protocol::Statuses::OK) },
      { :param => Sizes::INTEGER, :return => pack_integer(@session) }
    ].map! &SOCKET_RECV_EXPECTATION

    expect_sequence @socket, chain, 'response'

    result = @protocol.db_delete(@socket, @session, @database)

    assert_equal @session, result[:session]
  end

  def test_db_exist
    inputs = sequence('inputs')
    @socket.expects(:write).with(pack_byte(@protocol::Operations::DB_EXIST)).in_sequence(inputs)
    @socket.expects(:write).with(pack_integer(@session)).in_sequence(inputs)
    @socket.expects(:write).with(pack_string(@database)).in_sequence(inputs)

    chain = [
      { :param => Sizes::BYTE,    :return => pack_byte(@protocol::Statuses::OK) },
      { :param => Sizes::INTEGER, :return => pack_integer(@session) },
      { :param => Sizes::BYTE,    :return => pack_byte(1) }
    ].map! &SOCKET_RECV_EXPECTATION

    expect_sequence @socket, chain, 'response'

    result = @protocol.db_exist(@socket, @session, @database)

    assert_equal @session, result[:session]
    assert_equal 1, result[:message_content][:result]
  end

  def test_db_open
    inputs = sequence('inputs')
    @socket.expects(:write).with(pack_byte(@protocol::Operations::DB_OPEN)).in_sequence(inputs)
    @socket.expects(:write).with(pack_integer(@protocol::NEW_SESSION)).in_sequence(inputs)
    @socket.expects(:write).with(pack_string(@driver_name)).in_sequence(inputs)
    @socket.expects(:write).with(pack_string(@driver_version)).in_sequence(inputs)
    @socket.expects(:write).with(pack_short(@protocol_version)).in_sequence(inputs)
    @socket.expects(:write).with(pack_string(@database)).in_sequence(inputs)
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

  def test_record_create_document_synchronous
    cluster_id = 35
    cluster_position = 1726

    record = {
      :document => {
        :key1 => 'value1',
        :key2 => 'value2'
      }
    }

    inputs = sequence('inputs')
    @socket.expects(:write).with(pack_byte(@protocol::Operations::RECORD_CREATE)).in_sequence(inputs)
    @socket.expects(:write).with(pack_integer(@session)).in_sequence(inputs)
    @socket.expects(:write).with(pack_short(cluster_id)).in_sequence(inputs)
    @socket.expects(:write).with(pack_string(@protocol.serializer.serialize(record))).in_sequence(inputs)
    @socket.expects(:write).with(pack_byte('d'.ord)).in_sequence(inputs)
    @socket.expects(:write).with(pack_byte(@protocol::SyncModes::SYNC)).in_sequence(inputs)

    chain = [
      pack_byte(@protocol::Statuses::OK),
      pack_integer(@session),
      pack_long(cluster_position)
    ].map! &SOCKET_RECV_EXPECTATION

    expect_sequence @socket, chain, 'response'

    result = @protocol.record_create(@socket, @session, cluster_id, record)

    assert_equal cluster_id, result[:message_content][:cluster_id]
    assert_equal cluster_position, result[:message_content][:cluster_position]
  end

  def test_record_delete_synchronous
    cluster_id = 35
    cluster_position = 1726
    version = 0

    inputs = sequence('inputs')
    @socket.expects(:write).with(pack_byte(@protocol::Operations::RECORD_DELETE)).in_sequence(inputs)
    @socket.expects(:write).with(pack_integer(@session)).in_sequence(inputs)
    @socket.expects(:write).with(pack_short(cluster_id)).in_sequence(inputs)
    @socket.expects(:write).with(pack_long(cluster_position)).in_sequence(inputs)
    @socket.expects(:write).with(pack_integer(version)).in_sequence(inputs)
    @socket.expects(:write).with(pack_byte(@protocol::SyncModes::SYNC)).in_sequence(inputs)

    chain = [
      pack_byte(@protocol::Statuses::OK),
      pack_integer(@session),
      pack_byte(1)
    ].map! &SOCKET_RECV_EXPECTATION

    expect_sequence @socket, chain, 'response'

    result = @protocol.record_delete(@socket, @session, cluster_id, cluster_position, version)

    assert_equal 1, result[:message_content][:result]
  end

  def test_record_load
    cluster_id = 3
    cluster_position = 6

    inputs = sequence('inputs')
    @socket.expects(:write).with(pack_byte(@protocol::Operations::RECORD_LOAD)).in_sequence(inputs)
    @socket.expects(:write).with(pack_integer(@session)).in_sequence(inputs)
    @socket.expects(:write).with(pack_short(cluster_id)).in_sequence(inputs)
    @socket.expects(:write).with(pack_long(cluster_position)).in_sequence(inputs)

    chain = [
      pack_byte(@protocol::Statuses::OK),
      pack_integer(@session),
      pack_byte(@protocol::PayloadStatuses::NO_RECORDS)
    ].map! &SOCKET_RECV_EXPECTATION

    expect_sequence @socket, chain, 'response'

    result = @protocol.record_load(@socket, @session, OrientDbClient::Rid.new(cluster_id, cluster_position))

    assert_equal @session, result[:session]
  end

  def test_record_update_document_synchronous
    cluster_id = 35
    cluster_position = 1726
    record_version_policy = -2
    record_version = 0

    record = {
      :document => {
        :key1 => 'value1',
        :key2 => 'value2',
        :key3 => 'value3'
      }
    }

    inputs = sequence('inputs')
    @socket.expects(:write).with(pack_byte(@protocol::Operations::RECORD_UPDATE)).in_sequence(inputs)
    @socket.expects(:write).with(pack_integer(@session)).in_sequence(inputs)
    @socket.expects(:write).with(pack_short(cluster_id)).in_sequence(inputs)
    @socket.expects(:write).with(pack_long(cluster_position)).in_sequence(inputs)
    @socket.expects(:write).with(pack_string(@protocol.serializer.serialize(record))).in_sequence(inputs)
    @socket.expects(:write).with(pack_integer(record_version_policy)).in_sequence(inputs)
    @socket.expects(:write).with(pack_byte('d'.ord)).in_sequence(inputs)
    @socket.expects(:write).with(pack_byte(@protocol::SyncModes::SYNC)).in_sequence(inputs)

    chain = [
      pack_byte(@protocol::Statuses::OK),
      pack_integer(@session),
      pack_integer(record_version)
    ].map! &SOCKET_RECV_EXPECTATION

    expect_sequence @socket, chain, 'response'

    result = @protocol.record_update(@socket, @session, cluster_id, cluster_position, record)

    assert_equal record_version, result[:message_content][:record_version]
  end

  def test_db_reload
    inputs = sequence('inputs')
    @socket.expects(:write).with(pack_byte(@protocol::Operations::DB_RELOAD)).in_sequence(inputs)
    @socket.expects(:write).with(pack_integer(@session)).in_sequence(inputs)

    # recv chain
    chain = [
      { :param => Sizes::BYTE,    :return => pack_byte(@protocol::Statuses::OK) },
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

    chain.map! &SOCKET_RECV_EXPECTATION
    # End recv chain

    expect_sequence @socket, chain, 'response'

    result = @protocol.db_reload(@socket, @session)
    assert_equal @session, result[:session]
    assert_equal @clusters.length, result[:message_content][:clusters].length

    @clusters.each_with_index do |c, i|
      assert_equal @clusters[i][:id], result[:message_content][:clusters][i][:id]
      assert_equal @clusters[i][:name], result[:message_content][:clusters][i][:name]
      assert_equal @clusters[i][:type], result[:message_content][:clusters][i][:type]
    end
  end

  def test_db_size
    size = 1563467

    inputs = sequence('inputs')
    @socket.expects(:write).with(pack_byte(@protocol::Operations::DB_SIZE)).in_sequence(inputs)
    @socket.expects(:write).with(pack_integer(@session)).in_sequence(inputs)

    chain = [
      { :param => Sizes::BYTE,    :return => pack_byte(@protocol::Statuses::OK) },
      { :param => Sizes::INTEGER, :return => pack_integer(@session) },
      { :param => Sizes::LONG,    :return => pack_long(size) }
    ].map! &SOCKET_RECV_EXPECTATION

    expect_sequence @socket, chain, 'response'

    result = @protocol.db_size(@socket, @session)
    assert_equal @session, result[:session]
    assert_equal size, result[:message_content][:size]
  end
end
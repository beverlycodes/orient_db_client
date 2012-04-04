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
	end

  def socket_receives(request)
    @socket.expects(:write).with(request).returns(nil)
  end

  def test_connect
    request = OrientDbClient::NetworkMessage.new { |m|
      m.add :byte,    @protocol::Operations::CONNECT
      m.add :integer, @protocol::NEW_SESSION
      m.add :string,  @driver_name
      m.add :string,  @driver_version
      m.add :short,   @protocol_version
      m.add :string,  nil #client-id
      m.add :string,  @user
      m.add :string,  @password
    }.pack

    socket_receives(request)

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

    request = OrientDbClient::NetworkMessage.new { |m|
      m.add :byte,    @protocol::Operations::COUNT
      m.add :integer, @session
      m.add :string,  cluster_name
    }.pack

    socket_receives(request)

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

    command = OrientDbClient::NetworkMessage.new { |m|
      m.add :string,  'com.orientechnologies.orient.core.sql.query.OSQLSynchQuery'
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

  def test_datacluster_add_logical
    type = :logical       # Use symbol here, please
    container = 15

    new_cluster_number = 20

    request = OrientDbClient::NetworkMessage.new { |m|
      m.add :byte,    @protocol::Operations::DATACLUSTER_ADD
      m.add :integer, @session
      m.add :string,  type.to_s.upcase
      m.add :integer, container  
    }.pack

    socket_receives(request)

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

    request = OrientDbClient::NetworkMessage.new { |m|
      m.add :byte,    @protocol::Operations::DATACLUSTER_ADD
      m.add :integer, @session
      m.add :string,  type.to_s.upcase
      m.add :string,  name
    }.pack

    socket_receives(request)

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

    request = OrientDbClient::NetworkMessage.new { |m|
      m.add :byte,    @protocol::Operations::DATACLUSTER_ADD
      m.add :integer, @session
      m.add :string,  type
      m.add :string,  name  
      m.add :string,  file_name
      m.add :integer, size
    }.pack

    socket_receives(request)

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

    request = OrientDbClient::NetworkMessage.new { |m|
      m.add :byte,    @protocol::Operations::DATACLUSTER_DATARANGE
      m.add :integer, @session
      m.add :short,   cluster_id
    }.pack

    socket_receives(request)

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

    request = OrientDbClient::NetworkMessage.new { |m|
      m.add :byte,    @protocol::Operations::DATACLUSTER_REMOVE
      m.add :integer, @session
      m.add :short,   id
    }.pack

    socket_receives(request)

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
    request = OrientDbClient::NetworkMessage.new { |m|
      m.add :byte,    @protocol::Operations::DB_CLOSE
      m.add :integer, @session
    }.pack

    socket_receives(request)
    @socket.expects(:closed?).returns(true)

    result = @protocol.db_close(@socket, @session)

    assert result
  end

  def test_db_countrecords
    count = 26345

    request = OrientDbClient::NetworkMessage.new { |m|
      m.add :byte,    @protocol::Operations::DB_COUNTRECORDS
      m.add :integer, @session
    }.pack

    socket_receives(request)

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

    request = OrientDbClient::NetworkMessage.new { |m|
      m.add :byte,    @protocol::Operations::DB_CREATE
      m.add :integer, @session
      m.add :string,  @database
      m.add :string,  storage_type
    }.pack

    socket_receives(request)

    chain = [
      pack_byte(@protocol::Statuses::OK),
      pack_integer(@session)
    ].map! &SOCKET_RECV_EXPECTATION

    expect_sequence @socket, chain, 'response'

    result = @protocol.db_create(@socket, @session, @database, storage_type)

    assert_equal @session, result[:session]
  end

  def test_db_delete
    request = OrientDbClient::NetworkMessage.new { |m|
      m.add :byte,    @protocol::Operations::DB_DELETE
      m.add :integer, @session
      m.add :string,  @database
    }.pack

    socket_receives(request)

    chain = [
      { :param => Sizes::BYTE,    :return => pack_byte(@protocol::Statuses::OK) },
      { :param => Sizes::INTEGER, :return => pack_integer(@session) }
    ].map! &SOCKET_RECV_EXPECTATION

    expect_sequence @socket, chain, 'response'

    result = @protocol.db_delete(@socket, @session, @database)

    assert_equal @session, result[:session]
  end

  def test_db_exist
    request = OrientDbClient::NetworkMessage.new { |m|
      m.add :byte,    @protocol::Operations::DB_EXIST
      m.add :integer, @session
      m.add :string,  @database
    }.pack

    socket_receives(request)

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
    request = OrientDbClient::NetworkMessage.new { |m|
      m.add :byte,    @protocol::Operations::DB_OPEN
      m.add :integer, @protocol::NEW_SESSION
      m.add :string,  @driver_name
      m.add :string,  @driver_version
      m.add :short,   @protocol_version
      m.add :string,  nil #client-id
      m.add :string,  @database
      m.add :string,  @user
      m.add :string,  @password
    }.pack

    socket_receives(request)

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

    request = OrientDbClient::NetworkMessage.new { |m|
      m.add :byte,    @protocol::Operations::RECORD_CREATE
      m.add :integer, @session
      m.add :short,   cluster_id
      m.add :string,  @protocol.serializer.serialize(record)
      m.add :byte,    'd'.ord
      m.add :byte,    @protocol::SyncModes::SYNC
    }.pack

    socket_receives(request)

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

    request = OrientDbClient::NetworkMessage.new { |m|
      m.add :byte,    @protocol::Operations::RECORD_DELETE
      m.add :integer, @session
      m.add :short,   cluster_id
      m.add :long,    cluster_position
      m.add :integer, version
      m.add :byte,    @protocol::SyncModes::SYNC
    }.pack

    socket_receives(request)

    chain = [
      pack_byte(@protocol::Statuses::OK),
      pack_integer(@session),
      pack_byte(1)
    ].map! &SOCKET_RECV_EXPECTATION

    expect_sequence @socket, chain, 'response'

    result = @protocol.record_delete(@socket, @session, cluster_id, cluster_position, version)

    assert_equal 1, result[:message_content][:payload_status]
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
    }.pack

    socket_receives(request)

    chain = [
      pack_byte(@protocol::Statuses::OK),
      pack_integer(@session),
      pack_byte(@protocol::PayloadStatuses::NULL),
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

    request = OrientDbClient::NetworkMessage.new { |m|
      m.add :byte,    @protocol::Operations::RECORD_UPDATE
      m.add :integer, @session
      m.add :short,   cluster_id
      m.add :long,    cluster_position
      m.add :string,  @protocol.serializer.serialize(record)
      m.add :integer, record_version_policy
      m.add :byte,    'd'.ord
      m.add :byte,    @protocol::SyncModes::SYNC
    }.pack

    socket_receives(request)

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
    request = OrientDbClient::NetworkMessage.new { |m|
      m.add :byte,    @protocol::Operations::DB_RELOAD
      m.add :integer, @session
    }.pack

    socket_receives(request)

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

    request = OrientDbClient::NetworkMessage.new { |m|
      m.add :byte,    @protocol::Operations::DB_SIZE
      m.add :integer, @session
    }.pack

    socket_receives(request)

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
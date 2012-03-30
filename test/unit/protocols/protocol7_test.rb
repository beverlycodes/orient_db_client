require File.join File.dirname(__FILE__), '..', '..', 'test_helper'

class TestProtocol7 < MiniTest::Unit::TestCase
  include ConnectionHelper
  include ExpectationHelper
  include ProtocolHelper

  SOCKET_RECV_EXPECTATION = lambda do |r|
    expectation = { :method => :recv }

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

    result = @protocol.datacluster_add(@socket, @session, {
      :type => type,
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

    result = @protocol.datacluster_add(@socket, @session, {
      :type => type,
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

    result = @protocol.datacluster_add(@socket, @session, {
      :type => type,
      :name => name,
      :file_name => file_name,
      :initial_size => size
    })

    assert_equal @session, result[:session]
    assert_equal new_cluster_number, result[:message_content][:new_cluster_number]
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
    db_type = 'graph'
    storage_type = 'local'

    request = OrientDbClient::NetworkMessage.new { |m|
      m.add :byte,    @protocol::Operations::DB_CREATE
      m.add :integer, @session
      m.add :string,  @database
      m.add :string,  db_type
      m.add :string,  storage_type
    }.pack

    socket_receives(request)

    chain = [
      pack_byte(@protocol::Statuses::OK),
      pack_integer(@session)
    ].map! &SOCKET_RECV_EXPECTATION

    expect_sequence @socket, chain, 'response'

    result = @protocol.db_create(@socket, @session, @database, db_type, storage_type)

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
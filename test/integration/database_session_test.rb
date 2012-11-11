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

  def ensure_cluster_exists(session, cluster_name)
    if session.cluster_exists?(cluster_name)
      cluster = session.get_cluster(cluster_name)
      cluster[:id]
    else
      session.create_physical_cluster(cluster_name)
    end
  end

  def ensure_cluster_does_not_exist(session, cluster_name)
    if session.cluster_exists?(cluster_name)
      session.delete_cluster(session.get_cluster(cluster_name)[:id]) 
      session.reload     # I know.  Ridiculous
    end
  end

  # The protocol documentation for DB_CLOSE is very ambiguous.
  # As such, this test doesn't really do anything that makes sense...
  def test_close
    @session.close

    refute @connection.closed?
  end

  def test_cluster_exists?
    # Preform a bit of pre-test cleanup
    # FIXME: This won't be needed once we have some kind of DB clean in place
    cluster = @session.get_cluster("OTest")
    @session.delete_cluster(cluster[:id]) unless cluster.nil?

    assert @session.cluster_exists?(0)
    refute @session.cluster_exists?("OTest")
  end

  def test_query
    result = @session.query("SELECT FROM OUser")

    assert_equal 3, result.length

    result[0].tap do |record|
      assert_equal 0, record[:format]
      assert_equal 5, record[:cluster_id]
      assert_equal 0, record[:cluster_position]

      record[:document].tap do |doc|
        assert_equal 'admin', doc['name']
        assert_equal 'ACTIVE', doc['status']

        doc['roles'].tap do |roles|
          assert roles.is_a?(Array), "expected Array, but got #{roles.class}"

          assert roles[0].is_a?(OrientDbClient::Rid)
          assert_equal 4, roles[0].cluster_id
          assert_equal 0, roles[0].cluster_position
        end
      end
    end
  end

  def test_count
    result = @session.count("ORole")

    assert_equal @session.id, result[:session]
    assert_equal 3, result[:message_content][:record_count]
  end

  def test_create_cluster
    cluster = "OTest"
    ensure_cluster_does_not_exist(@session, cluster)

    new_cluster = @session.create_physical_cluster(cluster)

    assert_equal 8, new_cluster

    assert @session.cluster_exists?(cluster)

    # Cleanup
    # FIXME: Necessary due to lack of DB clean strategy
    @session.delete_cluster(new_cluster) unless new_cluster.nil?
  end

  def test_create_and_delete_record
    return
    cluster = "OTest"

    ensure_cluster_exists(@session, cluster)
    @session.reload

    cluster_id = @session.get_cluster(cluster)[:id]
    record = { :key1 => "value1" }

    rid = @session.create_record(cluster_id, record)
    created_record = @session.load_record(rid)

    assert_equal cluster_id, rid.cluster_id
    assert_equal 0, rid.cluster_position

    refute_nil created_record
    refute_nil created_record[:document]['key1']

    assert_equal record[:key1], created_record[:document]['key1']

    assert @session.delete_record(rid, created_record[:record_version])
    assert_nil @session.load_record(rid)

    ensure_cluster_does_not_exist(@session, cluster)
  end

  def test_delete_cluster
    # FIXME: Messy due to lack of a proper DB load/clean strategy

    cluster_id = ensure_cluster_exists(@session, "OTest")
   
    result = @session.delete_cluster(cluster_id)

    refute @session.cluster_exists?("OTest")
  end

  def test_get_cluster_by_id
    cluster_id = 0
    cluster_name = 'internal'

    result = @session.get_cluster(cluster_id)

    refute_nil result
    assert_equal cluster_id, result[:id]
    assert_equal cluster_name, result[:name]
  end

  def test_get_cluster_by_name
    cluster_id = 0
    cluster_name = 'internal'

    result = @session.get_cluster(cluster_name)

    refute_nil result
    assert_equal cluster_id, result[:id]
    assert_equal cluster_name, result[:name]
  end

  def test_get_cluster_noexist
    cluster_id = 9000

    result = @session.get_cluster(cluster_id)

    assert_nil result
  end

  def test_get_cluster_datarange
    cluster_id = 0
    expected_begin = 0
    expected_end = 2

    result = @session.get_cluster_datarange(cluster_id)

    result[:message_content].tap do |m|
      assert expected_begin, m[:begin]
      assert expected_end, m[:end]
    end
  end

  def test_load_record
    record = @session.load_record("#4:0")

    assert_equal 4, record[:cluster_id]
    assert_equal 0, record[:cluster_position]

    record[:document].tap do |doc|
      assert_equal 'admin', doc['name']
      assert_equal 1, doc['mode']
      assert doc['rules'].is_a?(Hash), "expected Hash, but got #{doc['rules'].class}"
      
    end
  end

  def test_reload
    cluster = "OTest"

    # FIXME: Messy due to lack of a proper DB load/clean strategy
    ensure_cluster_does_not_exist(@session, cluster)

    assert_nil @session.get_cluster(cluster)
    @session.create_physical_cluster(cluster)

    @session.reload

    refute_nil @session.get_cluster(cluster)

    # Cleanup
    # FIXME: Necessary due to lack of DB clean strategy
    @session.delete_cluster(@session.get_cluster(cluster)[:id])
  end
end

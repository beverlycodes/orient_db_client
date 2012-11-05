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

  def test_query
    result = @session.query("SELECT FROM OUser")

    assert_equal @session.id, result[:session]

    result[:message_content].tap do |content|
      assert_equal 3, content.length
  
      content[0].tap do |record|
        assert_equal 0, record[:format]
        assert_equal 4, record[:cluster_id]
        assert_equal 0, record[:cluster_position]

        record[:document].tap do |doc|
          assert_equal 'admin', doc['name']
          assert_equal 'ACTIVE', doc['status']

          doc['roles'].tap do |roles|
            assert roles.is_a?(Array), "expected Array, but got #{roles.class}"

            assert roles[0].is_a?(OrientDbClient::Rid)
            assert_equal 3, roles[0].cluster_id
            assert_equal 0, roles[0].cluster_position
          end
        end
      end
    end
  end

  def test_load_record
    result = @session.load_record("#4:0")

    assert_equal @session.id, result[:session]

    result[:message_content].tap do |record|
      assert_equal 4, record[:cluster_id]
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

end

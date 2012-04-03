require File.join File.dirname(__FILE__), '..', 'test_helper'

class TestRid < MiniTest::Unit::TestCase

  def setup
    @cluster_id = 3
    @cluster_position = 5
    @reference = "##{@cluster_id}:#{@cluster_position}"
  end

  def test_new_with_string
    rid = OrientDbClient::Rid.new(@reference)

    assert_equal @reference, rid.to_s
    assert_equal @cluster_id, rid.cluster_id
    assert_equal @cluster_position, rid.cluster_position
  end

  def test_new_with_components
    rid = OrientDbClient::Rid.new(@cluster_id, @cluster_position)

    assert_equal @reference, rid.to_s
    assert_equal @cluster_id, rid.cluster_id
    assert_equal @cluster_position, rid.cluster_position
  end

  def test_null
    rid = OrientDbClient::Rid.new

    assert_equal '#', rid.to_s
  end
end
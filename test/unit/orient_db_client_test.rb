require File.join File.dirname(__FILE__), '..', 'test_helper'
require 'socket'

class TestOrientDbClient < MiniTest::Unit::TestCase
  include ConnectionHelper

  def test_exception_on_unsupported_protocol
    bad_protocol = -1

    exp = assert_raises(OrientDbClient::UnsupportedProtocolError) do
      mock_connect_to_orientdb(bad_protocol)
    end

    assert_equal "The host reports protocol version #{bad_protocol}, which is not currently supported by this driver.", exp.message
  end
end

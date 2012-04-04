module ConnectionHelper
  def connect_to_orientdb(options)
		OrientDbClient.connect(options["host"], {
      port: options["port"],
      user: options["user"],
      password: options["password"]
    })
  end

  def mock_connect_to_orientdb(version, socket = nil)
    mock_socket = socket || MiniTest::Mock.new
    connection = nil

    begin
      TCPSocket.stubs(:open).returns(mock_socket)

      mock_socket.stubs(:read).returns([version].pack('s>'))
      mock_socket.stubs(:close)

      connection = connect_to_orientdb({})
    ensure
      TCPSocket.unstub(:open)
    end

    connection
  end
end
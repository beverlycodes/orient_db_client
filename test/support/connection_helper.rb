module ConnectionHelper
  def connect_to_orientdb(options)
    OrientDbClient.connect(options["host"], {
      port: options["port"]
    })
  end

  def mock_connect_to_orientdb(version, socket = nil)
    mock_socket = socket || mock()
    mock_socket.stubs(:pos).returns(false)

    begin
      TCPSocket.stubs(:open).returns(mock_socket)

      mock_socket.stubs(:read).returns(BinData::Int16be.new(version).to_binary_s)
      mock_socket.stubs(:close)

      connection = connect_to_orientdb({})
    ensure
      TCPSocket.unstub(:open)
    end

    connection
  end
end

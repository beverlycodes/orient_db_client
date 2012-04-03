require "orient_db_client/connection"
require "orient_db_client/version"
require "orient_db_client/rid"

require "socket"

module OrientDbClient
	def connect(host, options = {})
		options = { port: 2424 }.merge(options)

		s = TCPSocket.open(host, options[:port])

		protocol = s.read(2).unpack('s>').first

		Connection.new(s, protocol)
	end
	module_function :connect
end

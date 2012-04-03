require File.join(File.dirname(__FILE__), 'protocols', 'protocol7')
require File.join(File.dirname(__FILE__), 'protocols', 'protocol9')

module OrientDbClient
	class ProtocolFactory

		PROTOCOLS = {
			'7' => Protocols::Protocol7,
            '9' => Protocols::Protocol9
		}

		def self.get_protocol(version)
			PROTOCOLS[version.to_s] or raise UnsupportedProtocolError.new(version)
		end
	end
end
require File.join(File.dirname(__FILE__), 'protocols', 'protocol7')
require File.join(File.dirname(__FILE__), 'protocols', 'protocol12')

module OrientDbClient
	class ProtocolFactory

    # Orient server 1.0 supports Protocols 7 and 9.
    # Since Protocols 10 and 11 are not implemented by this client,
    # protocol 9 is substituted to allow connections to succeed.

		PROTOCOLS = {
			'7' => Protocols::Protocol7,
      '12' => Protocols::Protocol12
		}

		def self.get_protocol(version)
			PROTOCOLS[version.to_s] or raise UnsupportedProtocolError.new(version)
		end
	end
end
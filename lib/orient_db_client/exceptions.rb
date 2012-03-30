module OrientDbClient
	class UnsupportedProtocolError < StandardError
		def initialize(version)
			super "The host reports protocol version #{version}, which is not currently supported by this driver."
		end
	end

	class ProtocolError < StandardError
		attr_reader	:session
		attr_reader	:exception_class

		def initialize(session, *exceptions)
			@session
			@exception_class = exceptions[0][:exception_class]

			super exceptions.map { |exp| [ exp[:exception_class], exp[:exception_message] ].reject { |s| s.nil? }.join(': ') }.join("\n")
		end
	end
end
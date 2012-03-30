module OrientDbClient
	class Session
		attr_reader :id

		def initialize(id, connection)
			@id = id
			@connection = connection
		end
	end
end
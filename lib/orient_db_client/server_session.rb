require 'orient_db_client/session'

module OrientDbClient
	class ServerSession < Session
		def create_database(database, options = {})
			@connection.create_database(@id, database, options)
		end

		def database_exists?(database)
			@connection.database_exists?(@id, database)
		end

		def delete_database(database)
			@connection.delete_database(@id, database)
		end
	end
end
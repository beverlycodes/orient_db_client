require 'orient_db_client/session'

module OrientDbClient
	class DatabaseSession < Session
		attr_reader :clusters

		def initialize(id, connection, clusters = [])
			super id, connection

			@clusters = {}
			@clusters_by_name = {}

			clusters.each do |cluster|
				@clusters[cluster[:id]] = cluster
				@clusters_by_name[cluster[:name]] = cluster
			end
		end

		def close
			@connection.close_database(@id)
		end

		def cluster(id)
			if id.kind_of?(Fixnum)
				@clusters[id]
			else
				@clusters_by_name[id]
			end
		end

		def clusters
			@clusters.values
		end
	end
end
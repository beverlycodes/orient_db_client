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

		def command(text, options = {})
			@connection.command(@id, text, options)
		end

		def count(cluster_name)
			@connection.count(@id, cluster_name)
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

		def load_record(rid)
			@connection.load_record(@id, rid)
		end
	end
end
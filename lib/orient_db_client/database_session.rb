require 'orient_db_client/session'

module OrientDbClient
	class DatabaseSession < Session
		attr_reader :clusters

		def initialize(id, connection, clusters = [])
			super id, connection

			store_clusters(clusters)			
		end

		def close
			@connection.close_database(@id)
		end

		def cluster_exists?(cluster_id)
			@connection.cluster_exists?(@id, cluster_id)
		end

		def command(text, options = {})
			@connection.command(@id, text, options)
		end

		def count(cluster_name)
			@connection.count(@id, cluster_name)
		end

		def create_physical_cluster(name, options = {})
			options.merge!({ :name => name })

			@connection.create_cluster(@id, :physical, options)
		end

		def clusters
			@clusters.values
		end

		def delete_cluster(cluster_id)
			@connection.delete_cluster(@id, cluster_id)
		end

		def get_cluster(id)
			if id.kind_of?(Fixnum)
				@clusters[id]
			else
				@clusters_by_name[id.downcase]
			end
		end

		def get_cluster_datarange(cluster_id)
			@connection.get_cluster_datarange(@id, cluster_id)
		end

		def load_record(rid)
			@connection.load_record(@id, rid)
		end

		def reload
			@connection.reload(@id)
		end

		private

		def store_clusters(clusters)
			@clusters = {}
			@clusters_by_name = {}

			clusters.each do |cluster|
				@clusters[cluster[:id]] = cluster
				@clusters_by_name[cluster[:name].downcase] = cluster
			end
		end
	end
end
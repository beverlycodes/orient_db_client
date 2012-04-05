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

		def create_record(cluster_id, record)
			@connection.create_record(@id, cluster_id, record)
		end

		def delete_cluster(cluster_id)
			@connection.delete_cluster(@id, cluster_id)
		end

		def delete_record(rid_or_cluster_id, cluster_position_or_version, version = nil)
			if rid_or_cluster_id.is_a?(Fixnum)
				rid = OrientDbClient::Rid.new(rid_or_cluster_id, cluster_position)
				version = version
			else
				rid = rid_or_cluster_id
				version = cluster_position_or_version
			end
			
			@connection.delete_record(@id, rid, version)
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

		def load_record(rid_or_cluster_id, cluster_position = nil)
			if rid_or_cluster_id.is_a?(Fixnum)
				rid_or_cluster_id = OrientDbClient::Rid.new(rid_or_cluster_id, cluster_position)
			end
			
			@connection.load_record(@id, rid_or_cluster_id)[:message_content]
		end

		def query(text, options = {})
			@connection.query(@id, text, options)
		end

		def reload
			@connection.reload(@id)
		end

		def update_record(record, rid_or_cluster_id, cluster_position_or_version, version = nil)
			if rid_or_cluster_id.is_a?(Fixnum)
				rid = OrientDbClient::Rid.new(rid_or_cluster_id, cluster_position)
				version = version
			else
				rid = rid_or_cluster_id
				version = cluster_position_or_version
			end
			
			@connection.update_record(@id, rid, record, version)
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
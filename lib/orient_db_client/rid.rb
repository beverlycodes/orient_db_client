module OrientDbClient
    class Rid
        attr_reader :cluster_id
        attr_reader :cluster_position

        def initialize(cluster_id_or_rid = nil, cluster_position = nil)
            if cluster_id_or_rid.is_a?(String) && cluster_position.nil?
                rid = cluster_id_or_rid

                rid = rid[1..rid.length] if rid[0] == '#'

                @cluster_id, @cluster_position = rid.split(":")
            elsif cluster_id_or_rid.is_a?(OrientDbClient::Rid)
                rid = cluster_id_or_rid

                @cluster_id = rid.cluster_id
                @cluster_position = rid.cluster_position
            else
                @cluster_id = cluster_id_or_rid.nil? ? nil : cluster_id_or_rid
                @cluster_position = cluster_position.nil? ? nil : cluster_position
            end

            @cluster_id = @cluster_id.to_i unless @cluster_id.nil?
            @cluster_position = @cluster_position.to_i unless @cluster_position.nil?
        end

        def nil?
            @cluster_id.nil? || @cluster_position.nil?
        end

        def to_s
            if self.nil?
                '#'
            else
                "##{@cluster_id}:#{@cluster_position}"
            end
        end
    end
end
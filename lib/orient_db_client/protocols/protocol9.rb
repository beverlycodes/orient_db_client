require 'orient_db_client/network_message'
require 'orient_db_client/version'

module OrientDbClient
    module Protocols
        class Protocol9 < Protocol7
            VERSION = 9

            def self.command(socket, session, command, options = {})
                options[:query_class_name].tap do |qcn|
                    if qcn.nil? || qcn == 'com.orientechnologies.orient.core.sql.query.OSQLSynchQuery'
                        options[:query_class_name] = 'q' 
                    end
                end

                super socket, session, command, options
            end

            def self.record_load(socket, session, rid, options = {})
                socket.write NetworkMessage.new { |m|
                    m.add :byte,    Operations::RECORD_LOAD
                    m.add :integer, session
                    m.add :short,   rid.cluster_id
                    m.add :long,    rid.cluster_position
                    m.add :string,  ""
                    m.add :byte,    options[:ignore_cache] === true ? 1 : 0
                }.pack

                read_response(socket)

                { :session          => read_integer(socket),
                  :message_content  => read_record_load(socket) }
            end
        end
    end
end
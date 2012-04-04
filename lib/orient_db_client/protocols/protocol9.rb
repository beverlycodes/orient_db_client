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

            def self.db_create(socket, session, database, options = {})
                if options.is_a?(String)
                    options = { :storage_type => options }
                end

                options = {
                    :database_type => 'document',
                    :storage_type => 'local'
                }.merge(options)

                socket.write NetworkMessage.new { |m|
                    m.add :byte,    Operations::DB_CREATE
                    m.add :integer, session
                    m.add :string,  database
                    m.add :string,  options[:database_type]
                    m.add :string,  options[:storage_type]
                }.pack

                read_response(socket)

                { :session => read_integer(socket) }
            end

            def self.db_open(socket, database, options = {})
                socket.write NetworkMessage.new { |m|
                    m.add :byte,    Operations::DB_OPEN
                    m.add :integer, NEW_SESSION
                    m.add :string,  DRIVER_NAME
                    m.add :string,  DRIVER_VERSION
                    m.add :short,   self.version
                    m.add :integer, 0
                    m.add :string,  database
                    m.add :string,  options[:database_type] || "document"
                    m.add :string,  options[:user]
                    m.add :string,  options[:password]
                }.pack

                read_response(socket)

                { :session          => read_integer(socket),
                  :message_content  => read_db_open(socket) }
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
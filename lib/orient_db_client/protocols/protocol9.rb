require 'orient_db_client/network_message'
require 'orient_db_client/version'

require 'bindata'

module OrientDbClient
  module Protocols
    class Protocol9 < Protocol7
      VERSION = 12

      module Commands
        class DbCreate9 < BinData::Record
          endian :big

          int8            :operation,       :value => Protocol7::Operations::DB_CREATE
          int32           :session

          protocol_string :database
          protocol_string :database_type
          protocol_string :storage_type
        end

        class DbOpen9 < BinData::Record
          endian :big

          int8            :operation,       :value => Protocol7::Operations::DB_OPEN
          int32           :session,         :value => Protocol7::NEW_SESSION

          protocol_string :driver_name,     :value => Protocol7::DRIVER_NAME
          protocol_string :driver_version,  :value => Protocol7::DRIVER_VERSION
          int16           :protocol_version
          protocol_string :client_id
          protocol_string :database_name
          protocol_string :database_type
          protocol_string :user_name
          protocol_string :user_password
        end

        class RecordLoad9 < BinData::Record
          endian :big

          int8            :operation,         :value => Protocol7::Operations::RECORD_LOAD
          int32           :session

          int16           :cluster_id
          int64           :cluster_position
          protocol_string :fetch_plan
          int8            :ignore_cache,      :initial_value => 0
        end
      end

      def self.command(socket, session, command, options = {})
        options[:query_class_name].tap do |qcn|
          if qcn.is_a?(Symbol)
            qcn = case qcn
              when :query then 'q'
              when :command then 'c'
            end
          end

          if qcn.nil? || qcn == 'com.orientechnologies.orient.core.sql.query.OSQLSynchQuery'
            qcn = 'q' 
          end

          options[:query_class_name] = qcn
        end

        super socket, session, command, options
      end

      def self.db_create(socket, session, database, options = {})
          if options.is_a?(String)
            options = { :storage_type => options }
          end

          options = {
            :database_type => 'document'
          }.merge(options)

          super
      end

      def self.db_open(socket, database, options = {})
        puts "Connecting to db ith version: #{self.version}"
        command = Commands::DbOpen9.new :protocol_version => self.version,
                                        :database_name => database,
                                        :database_type => options[:database_type] || 'document',
                                        :user_name => options[:user],
                                        :user_password => options[:password]
        command.write(socket)

        read_response(socket)

        { :session          => read_integer(socket),
          :message_content  => read_db_open(socket) }
      end

      def self.record_load(socket, session, rid, options = {})
        command = Commands::RecordLoad9.new :session => session,
                                            :cluster_id => rid.cluster_id,
                                            :cluster_position => rid.cluster_position,
                                            :ignore_cache => options[:ignore_cache] === true ? 1 : 0
        command.write(socket)

        read_response(socket)

        { :session          => read_integer(socket),
          :message_content  => read_record_load(socket) }
      end

      private

      def self.make_db_create_command(*args)
          session = args.shift
          database = args.shift
          options = args.shift

          Commands::DbCreate9.new :session => session,
                                  :database => database,
                                  :database_type => options[:database_type].to_s,
                                  :storage_type => options[:storage_type]
      end

    end
  end
end
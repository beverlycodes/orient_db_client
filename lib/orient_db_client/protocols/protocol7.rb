require 'orient_db_client/network_message'
require 'orient_db_client/version'
require 'orient_db_client/deserializers/deserializer7'
require 'orient_db_client/serializers/serializer7'
require 'orient_db_client/exceptions'

module OrientDbClient
	module Protocols
		class Protocol7

			module SyncModes
				SYNC	= 0
				ASYNC 	= 1
			end

			module Operations
				CONNECT					= 2
				COUNT 					= 40
				DATACLUSTER_ADD			= 10
				DATACLUSTER_DATARANGE	= 13
				DATACLUSTER_REMOVE		= 11
				DB_CLOSE				= 5
				DB_COUNTRECORDS 		= 9
				DB_CREATE 				= 4
				DB_DELETE 				= 7
				DB_EXIST 				= 6
				DB_OPEN 				= 3
				DB_RELOAD 				= 73
				DB_SIZE 				= 8
				COMMAND					= 41
				RECORD_CREATE			= 31
				RECORD_DELETE			= 33
				RECORD_LOAD				= 30
				RECORD_UPDATE			= 32
			end

			module RecordTypes
				RAW			= 'b'.ord
				FLAT		= 'f'.ord
				DOCUMENT 	= 'd'.ord
			end

			module Statuses
				OK			= 0
				ERROR 		= 1
			end

			module PayloadStatuses
				NO_RECORDS	= 0
				RESULTSET	= 1
				PREFETCHED	= 2
				NULL		= 'n'.ord
				RECORD 		= 'r'.ord
				SERIALIZED 	= 'a'.ord
				COLLECTION	= 'l'.ord
			end

			module VersionControl
				INCREMENTAL		= -1
				NONE 			= -2
				ROLLBACK		= -3
			end

			VERSION = 7

			DRIVER_NAME 	= 'OrientDB Ruby Client'.freeze
			DRIVER_VERSION 	= OrientDbClient::VERSION

			COMMAND_CLASS = 'com.orientechnologies.orient.core.sql.OCommandSQL'.freeze
			QUERY_CLASS = 'com.orientechnologies.orient.core.sql.query.OSQLSynchQuery'.freeze

			NEW_SESSION = -1

			def self.command(socket, session, command, options = {})
				options = {
					:async =>				false,	# Async mode is not supported yet
					:query_class_name => QUERY_CLASS,
					:limit =>				-1
				}.merge(options);

				if options[:query_class_name].is_a?(Symbol)
					options[:query_class_name] = case options[:query_class_name]
						when :query then QUERY_CLASS
						when :command then COMMAND_CLASS
						else raise "Unsupported command class: #{options[:query_class_name]}"
					end
				end

				serialized_command = NetworkMessage.new { |m|
					m.add :string,		options[:query_class_name]
					m.add :string,		command
					m.add :integer,		options[:non_text_limit] || options[:limit]
					m.add :integer,		0
				}.pack

				socket.write NetworkMessage.new { |m|
					m.add :byte, 		Operations::COMMAND
					m.add :integer, 	session
					m.add :byte, 		options[:async] ? 'a' : 's'
					m.add :string,		serialized_command
				}.pack

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content 	=> read_command(socket) }
			end

			def self.connect(socket, options = {})
				socket.write NetworkMessage.new { |m|
					m.add :byte, 		Operations::CONNECT
					m.add :integer, 	NEW_SESSION
					m.add :string, 		DRIVER_NAME
					m.add :string, 		DRIVER_VERSION
					m.add :short,		self.version
					m.add :integer,		0
					m.add :string, 		options[:user]
					m.add :string, 		options[:password]
				}.pack

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content 	=> read_connect(socket) }
			end

			def self.count(socket, session, cluster_name)
				socket.write NetworkMessage.new { |m|
					m.add :byte, 		Operations::COUNT
					m.add :integer, 	session
					m.add :string, 		cluster_name
				}.pack

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content 	=> read_count(socket) }
			end

			def self.datacluster_add(socket, session, type, options)
				socket.write NetworkMessage.new { |m|
					type = type.downcase.to_sym if type.is_a?(String)
					type_string = type.to_s.upcase

					m.add :byte, 	Operations::DATACLUSTER_ADD
					m.add :integer,	session
					m.add :string,	type_string

					case type
						when :physical
							m.add :string,	options[:name]
							m.add :string,	options[:file_name]
							m.add :integer,	options[:initial_size] || -1
						when :logical
							m.add :integer,	options[:physical_cluster_container_id]
						when :memory
							m.add :string,	options[:name]
					end
				}.pack

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content 	=> read_datacluster_add(socket) }
			end

			def self.datacluster_datarange(socket, session, cluster_id)
				socket.write NetworkMessage.new { |m|
					m.add :byte, 	Operations::DATACLUSTER_DATARANGE
					m.add :integer,	session
					m.add :short,	cluster_id
				}.pack

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content 	=> read_datacluster_datarange(socket) }
			end

			def self.datacluster_remove(socket, session, cluster_id)
				socket.write NetworkMessage.new { |m|
					m.add :byte, 	Operations::DATACLUSTER_REMOVE
					m.add :integer,	session
					m.add :short,	cluster_id
				}.pack

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content 	=> read_datacluster_remove(socket) }
			end

			def self.db_close(socket, session = NEW_SESSION)
				socket.write NetworkMessage.new { |m|
					m.add :byte,	Operations::DB_CLOSE
					m.add :integer,	session
				}.pack

				return socket.closed?
			end

			def self.db_countrecords(socket, session)
				socket.write NetworkMessage.new { |m|
					m.add :byte,	Operations::DB_COUNTRECORDS
					m.add :integer, session
				}.pack

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content 	=> read_db_countrecords(socket) }
			end

			def self.db_create(socket, session, database, options = {})
        if options.is_a?(String) || options.is_a?(Symbol)
            options = { :storage_type => options }
        end

        options = { :storage_type => 'local' }.merge(options)

        options[:storage_type] = options[:storage_type].to_s

				socket.write make_db_create_message(session, database, options).pack

				read_response(socket)

				{ :session => read_integer(socket) }
			end

			def self.db_delete(socket, session, database)
				socket.write NetworkMessage.new { |m|
					m.add :byte, 	Operations::DB_DELETE
					m.add :integer,	session
					m.add :string,	database
				}.pack

				read_response(socket)

				{ :session => read_integer(socket) }
			end

			def self.db_exist(socket, session, database)
				socket.write NetworkMessage.new { |m|
					m.add :byte, 	Operations::DB_EXIST
					m.add :integer,	session
					m.add :string,	database
				}.pack

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content 	=> read_db_exist(socket) }
			end

			def self.db_open(socket, database, options = {})
				socket.write NetworkMessage.new { |m|
					m.add :byte,	Operations::DB_OPEN
					m.add :integer,	NEW_SESSION
					m.add :string, 	DRIVER_NAME
					m.add :string, 	DRIVER_VERSION
					m.add :short,	self.version
					m.add :integer,	0
					m.add :string,	database
					m.add :string, 	options[:user]
					m.add :string,	options[:password]
				}.pack

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content 	=> read_db_open(socket)	}
			end

			def self.db_reload(socket, session)
				socket.write NetworkMessage.new { |m|
					m.add :byte,	Operations::DB_RELOAD
					m.add :integer,	session
				}.pack

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content 	=> read_db_reload(socket)	}
			end

			def self.db_size(socket, session)
				socket.write NetworkMessage.new { |m|
					m.add :byte,	Operations::DB_SIZE
					m.add :integer,	session
				}.pack

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content 	=> read_db_size(socket) }
			end

			def self.record_create(socket, session, cluster_id, record)
				socket.write NetworkMessage.new { |m|
					m.add :byte,	Operations::RECORD_CREATE
					m.add :integer,	session
					m.add :short,	cluster_id
					m.add :string,	serializer.serialize(record)
					m.add :byte,	RecordTypes::DOCUMENT
					m.add :byte,	SyncModes::SYNC
				}.pack

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content	=> read_record_create(socket).merge({ :cluster_id => cluster_id }) }
			end

			def self.record_delete(socket, session, cluster_id, cluster_position, version)
				socket.write NetworkMessage.new { |m|
					m.add :byte,	Operations::RECORD_DELETE
					m.add :integer,	session
					m.add :short,	cluster_id
					m.add :long,	cluster_position
					m.add :integer,	version
					m.add :byte,	SyncModes::SYNC
				}.pack

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content	=> read_record_delete(socket) }
			end

			def self.record_load(socket, session, rid)
				socket.write NetworkMessage.new { |m|
					m.add :byte,	Operations::RECORD_LOAD
					m.add :integer,	session
					m.add :short,	rid.cluster_id
					m.add :long,	rid.cluster_position
					m.add :string,	""
				}.pack

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content 	=> read_record_load(socket) }
			end

			def self.record_update(socket, session, cluster_id, cluster_position, record, version = VersionControl::NONE)
				if version.is_a?(Symbol)
					version = case version
						when :none then VersionControl::NONE
						when :incremental then VersionControl::INCREMENTAL
						else VersionControl::NONE
					end
				end

				socket.write NetworkMessage.new { |m|
					m.add :byte,	Operations::RECORD_UPDATE
					m.add :integer,	session
					m.add :short,	cluster_id
					m.add :long,	cluster_position
					m.add :string,	serializer.serialize(record)
					m.add :integer,	version
					m.add :byte,	RecordTypes::DOCUMENT
					m.add :byte,	SyncModes::SYNC
				}.pack

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content	=> read_record_update(socket) }
			end

			def self.deserializer
				return OrientDbClient::Deserializers::Deserializer7.new
			end

			def self.serializer
				return OrientDbClient::Serializers::Serializer7.new
			end

			def self.version
				self::VERSION
			end

			private

			def self.make_db_create_message(*args)
				session = args.shift
				database = args.shift
				options = args.shift

				NetworkMessage.new { |m|
					m.add :byte,		Operations::DB_CREATE
					m.add :integer, session
					m.add :string,	database
					m.add :string,	options[:storage_type].to_s
				}
			end

			def self.read_byte(socket)
				socket.read(1).unpack('C').first
			end

			def self.read_count(socket)
				{ :record_count => read_long(socket) }
			end

			def self.read_clusters(socket)
				clusters = []

				read_short(socket).times do
					clusters << {
						:name 	=> read_string(socket),
						:id 	=> read_short(socket),
						:type 	=> read_string(socket)						
					}
				end

				clusters
			end

			def self.read_collection_record(socket)
				record = { :format		=> read_short(socket) }

				case record[:format]
				when 0
					record.merge!({
						:record_type		=> read_byte(socket),
				  		:cluster_id 		=> read_short(socket),
						:cluster_position	=> read_long(socket),
						:record_version 	=> read_integer(socket),
						:bytes				=> read_string(socket) })
				else
					raise "Unsupported record format: #{record[:format]}"
				end
			end

			def self.read_command(socket)
				result = []

				while (status = read_byte(socket)) != PayloadStatuses::NO_RECORDS
					case status
					when PayloadStatuses::NULL
						result.push(nil)
					when PayloadStatuses::COLLECTION
						collection = read_record_collection(socket)
						result.concat collection
						break
					else
						raise "Unsupported payload status: #{status}"
					end
				end

				result
			end

			def self.read_connect(socket)
				{ :session => read_integer(socket) }
			end

			def self.read_datacluster_add(socket)
				{ :new_cluster_number => read_short(socket) }
			end

			def self.read_datacluster_datarange(socket)
				{ :begin => read_long(socket),
				  :end =>	read_long(socket) }
			end

			def self.read_datacluster_remove(socket)
				{ :result => read_byte(socket) }
			end

			def self.read_db_countrecords(socket)
				{ :count => read_long(socket) }
			end

			def self.read_db_exist(socket)
				{ :result => read_byte(socket) }
			end

			def self.read_db_open(socket)
				{ :session 			=> read_integer(socket),
				  :clusters 		=> read_clusters(socket),
				  :cluster_config 	=> read_string(socket)	}
			end

			def self.read_db_reload(socket)
				{ :clusters => read_clusters(socket) }
			end

			def self.read_db_size(socket)
				{ :size => read_long(socket) }
			end

			def self.read_integer(socket)
				socket.read(4).unpack('l>').first
			end

			def self.read_long(socket)
				socket.read(8).unpack('q>').first
			end

			def self.read_record(socket)
				{ :bytes			=> read_string(socket),
				  :record_version 	=> read_integer(socket),
				  :record_type		=> read_byte(socket) }
			end

			def self.read_record_collection(socket)
				count = read_integer(socket)
				records = []

				count.times do
					record = read_collection_record(socket)
					record[:document] = deserializer.deserialize(record[:bytes])[:document]
					record.delete(:bytes)
					records << record
				end

				records
			end

			def self.read_record_create(socket)
				{ :cluster_position => read_long(socket) }
			end

			def self.read_record_delete(socket)
				{ :result => read_byte(socket) }
			end

			def self.read_record_load(socket)
				result = nil

				while (status = read_byte(socket)) != PayloadStatuses::NO_RECORDS
					case status
					when PayloadStatuses::RESULTSET
						record = record || read_record(socket)

						case record[:record_type]
						when 'd'.ord
							result = result || record
							result[:document] = deserializer.deserialize(record[:bytes])[:document]
						else
							raise "Unsupported record type: #{record[:record_type]}"
						end
					else
						raise "Unsupported payload status: #{status}"
					end
				end

				result
			end

			def self.read_record_update(socket)
				{ :record_version => read_integer(socket) }
			end

			def self.read_response(socket)
				result = read_byte(socket)

				raise_response_error(socket) unless result == Statuses::OK
			end

			def self.raise_response_error(socket)
				session = read_integer(socket)
				exceptions = []

				while (result = read_byte(socket)) == Statuses::ERROR
					exceptions << {
						:exception_class => read_string(socket),
						:exception_message => read_string(socket)
					}
				end

				if exceptions[0] && exceptions[0][:exception_class] == "com.orientechnologies.orient.core.exception.ORecordNotFoundException"
					raise RecordNotFound.new(session)
				else
					raise ProtocolError.new(session, *exceptions)
				end
			end

			def self.read_short(socket)
				socket.read(2).unpack('s>').first
			end

			def self.read_string(socket)
				length = read_integer(socket)
				
				length > 0 ? socket.read(length) : nil
			end
		end
	end
end
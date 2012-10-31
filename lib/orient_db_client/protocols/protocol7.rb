require 'orient_db_client/network_message'
require 'orient_db_client/version'
require 'orient_db_client/deserializers/deserializer7'
require 'orient_db_client/serializers/serializer7'
require 'orient_db_client/exceptions'

require 'bindata'

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
				CONFIG_GET        = 70
				CONFIG_SET        = 71
				CONFIG_LIST       = 72
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

			class ProtocolString < BinData::Primitive
				endian		:big
				
				int32		:len,	  :value => lambda { data.length }
				string 	:data,	:read_length => :len

				def get;   self.data; end
				def set(v) self.data = v; end
  		end

  		class QueryMessage < BinData::Record
  			endian :big

				protocol_string		:query_class_name
				protocol_string		:text
				int32 						:non_text_limit,			:initial_value => -1
				int32 						:serialized_params,		:value => 0
  		end

			module Commands
				class Command < BinData::Record
					endian :big

					int8		 				:operation, 			:value => Operations::COMMAND
					int32 					:session
					int8 						:mode,						:initial_value => 's'.ord

					protocol_string	:command_serialized
				end

				class Connect < BinData::Record
					endian :big

					int8		 				:operation, 			:value => Operations::CONNECT
					int32 					:session,					:value => NEW_SESSION
					protocol_string :driver,					:value => DRIVER_NAME
					protocol_string	:driver_version,	:value => DRIVER_VERSION
					int16						:version
					protocol_string	:client_id
					protocol_string	:user
					protocol_string :password
				end

				class Count < BinData::Record
					endian :big

					int8 						:operation,				:value => Operations::COUNT
					int32 					:session
					protocol_string	:cluster_name
				end

				class DataclusterAddLogical < BinData::Record
					endian :big

					int8 						:operation,				:value => Operations::DATACLUSTER_ADD
					int32 					:session
					protocol_string	:type, 						:value => 'LOGICAL'

					int32 					:physical_cluster_container_id
				end

				class DataclusterAddMemory < BinData::Record
					endian :big

					int8 						:operation,				:value => Operations::DATACLUSTER_ADD
					int32 					:session
					protocol_string	:type, 						:value => 'MEMORY'

					protocol_string	:name
				end

				class DataclusterAddPhysical < BinData::Record
					endian :big

					int8 						:operation,				:value => Operations::DATACLUSTER_ADD
					int32 					:session
					protocol_string	:type, 						:value => 'PHYSICAL'

					protocol_string	:name
					protocol_string :file_name
					int32 					:initial_size,		:initial_value => -1
				end

				class DataclusterDatarange < BinData::Record
					endian :big

					int8 		:operation,	:value => Operations::DATACLUSTER_DATARANGE
					int32 	:session
					int16 	:cluster_id
				end

				class DataclusterRemove < BinData::Record
					endian :big

					int8 		:operation, :value =>	Operations::DATACLUSTER_REMOVE
					int32 	:session
					Int16 	:cluster_id
				end

				class DbClose < BinData::Record
					endian :big

					int8 	:operation, :value =>	Operations::DB_CLOSE
					int32 :session
				end

				class DbCountRecords < BinData::Record
					endian :big

					int8 	:operation, :value =>	Operations::DB_COUNTRECORDS
					int32 :session
				end

				class DbCreate < BinData::Record
					endian :big

					int8		 				:operation, 			:value => Operations::DB_CREATE
					int32 					:session

					protocol_string :database
					protocol_string :storage_type
				end

				class DbDelete < BinData::Record
					endian :big

					int8		 				:operation, 			:value => Operations::DB_DELETE
					int32 					:session

					protocol_string :database
				end

				class DbExist < BinData::Record
					endian :big

					int8		 				:operation, 			:value => Operations::DB_EXIST
					int32 					:session

					protocol_string :database
				end

				class DbOpen < BinData::Record
					endian :big

					int8		 				:operation, 			:value => Operations::DB_OPEN
					int32 					:session,					:value => NEW_SESSION

					protocol_string :driver,					:value => DRIVER_NAME
					protocol_string	:driver_version,	:value => DRIVER_VERSION
					int16						:version
					protocol_string	:client_id
					protocol_string :database
					protocol_string	:user
					protocol_string :password
				end

				class DbReload < BinData::Record
					endian :big

					int8 	:operation, :value =>	Operations::DB_RELOAD
					int32 :session
				end

				class DbSize < BinData::Record
					endian :big

					int8 	:operation, :value =>	Operations::DB_SIZE
					int32 :session
				end

				class RecordCreate < BinData::Record
					endian :big

					int8 							:operation,				:value =>	Operations::RECORD_CREATE
					int32 						:session

					int16 						:cluster_id
					protocol_string		:record_content
					int8 							:record_type,			:value => RecordTypes::DOCUMENT
					int8 							:mode,						:value => SyncModes::SYNC
				end

				class RecordDelete < BinData::Record
					endian :big

					int8 	:operation,					:value =>	Operations::RECORD_DELETE
					int32 :session

					int16 :cluster_id
					int64 :cluster_position
					int32 :record_version
					int8 	:mode,							:value => SyncModes::SYNC
				end

				class RecordLoad < BinData::Record
					endian :big

					int8 						:operation,					:value =>	Operations::RECORD_LOAD
					int32 					:session

					int16 					:cluster_id
					int64 					:cluster_position
					protocol_string	:fetch_plan
				end

				class RecordUpdate < BinData::Record
					endian :big

					int8 						:operation,					:value =>	Operations::RECORD_UPDATE
					int32 					:session

					int16 					:cluster_id
					int64 					:cluster_position

					protocol_string :record_content
					int32 					:record_version
					int8 						:record_type,				:value => RecordTypes::DOCUMENT
					int8 						:mode,							:value => SyncModes::SYNC
				end
			end

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

				query = QueryMessage.new :query_class_name => options[:query_class_name],
																 :text => command,
																 :non_text_limit => options[:non_text_limit] || options[:limit]

				command = Commands::Command.new :session => session,
																				:mode => options[:async] ? 'a'.ord : 's'.ord,
																				:command_serialized => query.to_binary_s

				command.write(socket)

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content 	=> read_command(socket) }
			end

			def self.connect(socket, options = {})
				command = Commands::Connect.new :version => self.version,
																				:user => options[:user],
																				:password => options[:password]
				command.write(socket)

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content 	=> read_connect(socket) }
			end

			def self.count(socket, session, cluster_name)
				command = Commands::Count.new :session => session,
																				:cluster_name => cluster_name

				command.write(socket)

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content 	=> read_count(socket) }
			end

			def self.datacluster_add(socket, session, type, options)
				type = type.downcase.to_sym if type.is_a?(String)

				case type
				when :physical
					command = Commands::DataclusterAddPhysical.new :session => session,
																												 :name => options[:name],
																												 :file_name => options[:file_name],
																												 :initial_size => options[:initial_size] || -1
				when :logical
					command = Commands::DataclusterAddLogical.new :session => session,
																												:physical_cluster_container_id => options[:physical_cluster_container_id]
				when :memory
					command = Commands::DataclusterAddMemory.new :session => session,
																											 :name => options[:name]
				end

				command.write(socket)

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content 	=> read_datacluster_add(socket) }
			end

			def self.datacluster_datarange(socket, session, cluster_id)
				command = Commands::DataclusterDatarange.new :session => session,
																										 :cluster_id => cluster_id

				command.write(socket)

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content 	=> read_datacluster_datarange(socket) }
			end

			def self.datacluster_remove(socket, session, cluster_id)
				command = Commands::DataclusterRemove.new :session => session,
																										 :cluster_id => cluster_id
				command.write(socket)

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content 	=> read_datacluster_remove(socket) }
			end

			def self.db_close(socket, session = NEW_SESSION)
				command = Commands::DbClose.new :session => session
				command.write(socket)

				return socket.closed?
			end

			def self.db_countrecords(socket, session)
				command = Commands::DbCountRecords.new :session => session
				command.write(socket)

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

				make_db_create_command(session, database, options).write(socket)

				read_response(socket)

				{ :session => read_integer(socket) }
			end

			def self.db_delete(socket, session, database)
				command = Commands::DbDelete.new :session => session,
																							 :database => database
				command.write(socket)

				read_response(socket)

				{ :session => read_integer(socket) }
			end

			def self.db_exist(socket, session, database)
				command = Commands::DbExist.new :session => session,
																				 :database => database
				command.write(socket)

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content 	=> read_db_exist(socket) }
			end

			def self.db_open(socket, database, options = {})
				command = Commands::DbOpen.new :version => self.version,
																			 :database => database,
																			 :user => options[:user],
																			 :password => options[:password]
				command.write(socket)

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content 	=> read_db_open(socket)	}
			end

			def self.db_reload(socket, session)
				command = Commands::DbReload.new :session => session
				command.write(socket)

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content 	=> read_db_reload(socket)	}
			end

			def self.db_size(socket, session)
				command = Commands::DbSize.new :session => session
				command.write(socket)

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content 	=> read_db_size(socket) }
			end
			
			def self.config_get(socket, session)
			end

			def self.record_create(socket, session, cluster_id, record)
				command = Commands::RecordCreate.new :session => session,
																						 :cluster_id => cluster_id,
																						 :record_content => serializer.serialize(record)
				command.write(socket)

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content	=> read_record_create(socket).merge({ :cluster_id => cluster_id }) }
			end

			def self.record_delete(socket, session, cluster_id, cluster_position, version)
				command = Commands::RecordDelete.new :session => session,
																						 :cluster_id => cluster_id,
																						 :cluster_position => cluster_position,
																						 :record_version => version
				command.write(socket)

				read_response(socket)

				{ :session 			=> read_integer(socket),
				  :message_content	=> read_record_delete(socket) }
			end

			def self.record_load(socket, session, rid)
				command = Commands::RecordLoad.new :session => session,
																					 :cluster_id => rid.cluster_id,
																					 :cluster_position => rid.cluster_position
				command.write(socket)

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

				command = Commands::RecordUpdate.new :session => session,
																					 :cluster_id => cluster_id,
																					 :cluster_position => cluster_position,
																					 :record_content => serializer.serialize(record),
																					 :record_version => version
				command.write(socket)

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

			def self.make_db_create_command(*args)
				session = args.shift
				database = args.shift
				options = args.shift

				Commands::DbCreate.new :session => session,
															 :database => database,
															 :storage_type => options[:storage_type]
			end

			def self.read_byte(socket)
				BinData::Int8.read(socket).to_i
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
				BinData::Int32be.read(socket).to_i
			end

			def self.read_long(socket)
				BinData::Int64be.read(socket).to_i
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
				BinData::Int16be.read(socket).to_i
			end

			def self.read_string(socket)
				bin_length = read_integer(socket)
				return nil if bin_length < 0

				raise bin_length.inspect if bin_length < 0

				bin_str = socket.read(bin_length)
				bin_str.length > 0 ? bin_str : nil
			end
		end
	end
end
require 'orient_db_client/network_message'
require 'orient_db_client/version'

module OrientDbClient
	module Protocols
		class Protocol7

			module Operations
				CONNECT 						=	2
				DATACLUSTER_ADD			= 10
				DATACLUSTER_REMOVE	= 11
				DB_CLOSE 						=	5
				DB_COUNTRECORDS 		=	9
				DB_CREATE 					=	4
				DB_DELETE 					=	7
				DB_EXIST 						=	6
				DB_OPEN 						= 3
				DB_RELOAD 					=	73
				DB_SIZE							=	8
			end

			module Statuses
				OK			= 0
				ERROR 	= 1
			end

			VERSION 				=	7

			DRIVER_NAME 		=	'OrientDB Ruby Client'.freeze
			DRIVER_VERSION 	= OrientDbClient::VERSION

			NEW_SESSION 		=	-1

			def self.connect(socket, options = {})
				socket.write NetworkMessage.new { |m|
					m.add :byte, 			Operations::CONNECT
					m.add :integer, 	NEW_SESSION
					m.add :string, 		DRIVER_NAME
					m.add :string, 		DRIVER_VERSION
					m.add :short,			VERSION
					m.add :integer,		0
					m.add :string, 		options[:user]
					m.add :string, 		options[:password]
				}.pack

				read_response(socket)

				{ :session =>					read_integer(socket),
					:message_content =>	read_connect(socket)	}
			end

			def self.datacluster_add(socket, session, options)
				socket.write NetworkMessage.new { |m|
					type = options[:type]
					type = type.downcase.to_sym if type.is_a?(String)
					type_string = type.to_s.upcase

					m.add :byte, 		Operations::DATACLUSTER_ADD
					m.add :integer,	session
					m.add :string,	type_string

					case type
						when :physical
							m.add :string,	options[:name]
							m.add :string,	options[:file_name]
							m.add :integer,	options[:initial_size]
						when :logical
							m.add :integer,	options[:physical_cluster_container_id]
						when :memory
							m.add :string,	options[:name]
					end
				}.pack

				read_response(socket)

				{ :session => read_integer(socket),
					:message_content => read_datacluster_add(socket) }
			end

			def self.datacluster_remove(socket, session, cluster_id)
				socket.write NetworkMessage.new { |m|
					m.add :byte, 		Operations::DATACLUSTER_REMOVE
					m.add :integer,	session
					m.add :short,		cluster_id
				}.pack

				read_response(socket)

				{ :session => read_integer(socket),
					:message_content => read_datacluster_remove(socket) }
			end

			def self.db_close(socket, session = NEW_SESSION)
				socket.write NetworkMessage.new { |m|
					m.add :byte,		Operations::DB_CLOSE
					m.add :integer,	session
				}.pack

				return socket.closed?
			end

			def self.db_countrecords(socket, session)
				socket.write NetworkMessage.new { |m|
					m.add :byte,		Operations::DB_COUNTRECORDS
					m.add :integer, session
				}.pack

				read_response(socket)

				{ :session =>	read_integer(socket),
					:message_content => read_db_countrecords(socket) }
			end

			def self.db_create(socket, session, database, storage_type)
				socket.write NetworkMessage.new { |m|
					m.add :byte,		Operations::DB_CREATE
					m.add :integer, session
					m.add :string,	database
					m.add :string,	storage_type
				}.pack

				read_response(socket)

				{ :session =>	read_integer(socket) }
			end

			def self.db_delete(socket, session, database)
				socket.write NetworkMessage.new { |m|
					m.add :byte, 		Operations::DB_DELETE
					m.add :integer,	session
					m.add :string,	database
				}.pack

				read_response(socket)

				{ :session =>	read_integer(socket) }
			end

			def self.db_exist(socket, session, database)
				socket.write NetworkMessage.new { |m|
					m.add :byte, 		Operations::DB_EXIST
					m.add :integer,	session
					m.add :string,	database
				}.pack

				read_response(socket)

				{ :session =>	read_integer(socket),
					:message_content => read_db_exist(socket) }
			end

			def self.db_open(socket, database, options = {})
				socket.write NetworkMessage.new { |m|
					m.add :byte,		Operations::DB_OPEN
					m.add :integer,	NEW_SESSION
					m.add :string, 	DRIVER_NAME
					m.add :string, 	DRIVER_VERSION
					m.add :short,		VERSION
					m.add :integer,	0
					m.add :string,	database
					m.add :string, 	options[:user]
					m.add :string,	options[:password]
				}.pack

				read_response(socket)

				{ :session =>					read_integer(socket),
					:message_content =>	read_db_open(socket)	}
			end

			def self.db_reload(socket, session)
				socket.write NetworkMessage.new { |m|
					m.add :byte,		Operations::DB_RELOAD
					m.add :integer,	session
				}.pack

				read_response(socket)

				{ :session =>					read_integer(socket),
					:message_content =>	read_db_reload(socket)	}
			end

			def self.db_size(socket, session)
				socket.write NetworkMessage.new { |m|
					m.add :byte,		Operations::DB_SIZE
					m.add :integer,	session
				}.pack

				read_response(socket)

				{ :session =>	read_integer(socket),
					:message_content => read_db_size(socket) }
			end

			private

			def self.read_byte(socket)
				socket.recv(1).unpack('C').first
			end

			def self.read_clusters(socket)
				clusters = []

				read_short(socket).times do
					clusters << {
						:name =>			read_string(socket),
						:id =>				read_short(socket),
						:type =>			read_string(socket)						
					}
				end

				clusters
			end

			def self.read_connect(socket)
				{	:session =>		read_integer(socket) }
			end

			def self.read_datacluster_add(socket)
				{ new_cluster_number: read_short(socket) }
			end

			def self.read_datacluster_remove(socket)
				{	:result =>		read_byte(socket)	}
			end

			def self.read_db_countrecords(socket)
				{	:count =>		read_long(socket)	}
			end

			def self.read_db_exist(socket)
				{	:result =>		read_byte(socket)	}
			end

			def self.read_db_open(socket)
				{ :session =>					read_integer(socket),
					:clusters =>				read_clusters(socket),
					:cluster_config =>	read_string(socket)	}
			end

			def self.read_db_reload(socket)
				{	:clusters => read_clusters(socket) }
			end

			def self.read_db_size(socket)
				{ :size => read_long(socket) }
			end

			def self.read_integer(socket)
				socket.recv(4).unpack('l>').first
			end

			def self.read_long(socket)
				socket.recv(8).unpack('q>').first
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

				raise ProtocolError.new(session, *exceptions)
			end

			def self.read_short(socket)
				socket.recv(2).unpack('s>').first
			end

			def self.read_string(socket)
				length = read_integer(socket)
				length > 0 ? socket.recv(length) : nil
			end
		end
	end
end
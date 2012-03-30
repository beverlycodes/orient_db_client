require 'orient_db_client/database_session'
require 'orient_db_client/server_session'
require 'orient_db_client/protocol_factory'

module OrientDbClient
  class Connection
  	def initialize(socket, protocol_version, options = {})
  		@socket = socket
  		@protocol = (options[:protocol_factory] || ProtocolFactory).get_protocol(protocol_version)
      @sessions = {}
  	end

  	def close
      @socket.close
  	end

    def close_database(session)
      @protocol.db_close(@socket, session)
    end

    def closed?
      @socket.closed?
    end

    def create_database(session, database, options = {})
      options = {
        :type => "graph",         # Will be used in protocol version 8
        :storage_type => "local"
      }.merge(options)

      @protocol.db_create(@socket, session, database, options[:storage_type])
    end

    def database_exists?(session, database)
      response = @protocol.db_exist(@socket, session, database)

      response[:message_content][:result] == 1
    end

    def delete_database(session, database)
      @protocol.db_delete(@socket, session, database)
    end

    def open_server(options = {})
      response = @protocol.connect(@socket, options)
      session = response[:session]
      message_content = response[:message_content]

      @sessions[session] = ServerSession.new(message_content[:session], self)
    end

  	def open_database(database, options = {})
  		response = @protocol.db_open(@socket, database, options)
      session = response[:session]
      message_content = response[:message_content]

      @sessions[session] = DatabaseSession.new(message_content[:session], self, message_content[:clusters])
  	end
  end
end

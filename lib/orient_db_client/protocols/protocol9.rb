require 'orient_db_client/network_message'
require 'orient_db_client/version'

module OrientDbClient
    module Protocols
        class Protocol9 < Protocol7
            VERSION = 9
        end
    end
end
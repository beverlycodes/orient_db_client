require 'yaml'

module ServerConfig
		SERVER_CONFIG = YAML.load_file(File.join(File.dirname(__FILE__), "databases.yml"))
		SERVER_OPTIONS = SERVER_CONFIG["test"]
end

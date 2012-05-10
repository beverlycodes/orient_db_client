require "bindata"

module OrientDbClient
	class NetworkMessage 

		def initialize(&block)
			@components = []

			yield(self) unless block.nil?
		end

		def add(type, value)
			@components << { :type => :integer, :value => (value && value.length) || 0 } if type == :string
			@components << { :type => type, :value => value } unless type == :string && value.nil?
		end

		def pack()
			packing_list = @components.map do |c|
				case c[:type]
					when :byte then 'C'
					when :integer then 'l>'
					when :long  then 'q>'
					when :short then 's>'
					when :string, :raw_string then 'a*'
				end
			end

			content = @components.map do |c|
				value = c[:value]

				if c[:type] == :byte && value.is_a?(String)
					c[:value] = value.length > 0 ? value[0].ord : 0
				end

				c[:value]
			end

			content.pack packing_list.join(" ")
		end

		def to_s
			self.pack
		end
	end
end
module OrientDbClient
    module Types
        [ :BINARY,
          :BOOLEAN,
          :BYTE,
          :COLLECTION,
          :DATE,
          :DECIMAL,
          :DOCUMENT,
          :DOUBLE,
          :FLOAT,
          :LONG,
          :MAP,
          :RID,
          :SHORT,
          :STRING,
          :TIME ].each_with_index do |c, i|

            const_set c, i
        end
    end
end
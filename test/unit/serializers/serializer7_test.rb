require File.join File.dirname(__FILE__), '..', '..', 'test_helper'

class TestSerializer7 < MiniTest::Unit::TestCase
    def setup
        @serializer = OrientDbClient::Protocols::Protocol7.serializer
    end

    def test_serialize
        record = {
            :class => 'OClass',
            :structure => {
                'buffer'                => :binary,
                'true'                  => :boolean,
                'false'                 => :boolean,
                'byte'                  => :byte,
                'array'                 => :collection,
                'date_from_time'        => :date,
                'date_from_string'      => :date,
                'bigdec'                => :decimal,
                'doc'                   => :document,
                'double'                => :double,
                'float'                 => :float,
                'bignum'                => :long,
                'map'                   => :map,
                'rid'                   => :rid,
                'short'                 => :short,
                'string'                => :string,
                'time'                  => :time
            },
            :document => {
                'implicit_boolean_true' => true,
                'implicit_boolean_false' => false,
                'implicit_collection' => [ 3, 7, 14 ],
                'implicit_date' => Date.new(2012, 4, 3),
                'implicit_document' => { :document => { :sym_key => "embedded doc string" } },
                'implicit_double' => 6.6236,
                'implicit_integer' => 15,
                'implicit_long' => 7345723467317884556,
                'implicit_map' => { :key1 => 'value1', :key2 => 'value2' },
                'implicit_time' => Time.at(1296279468).utc,
                'implicit_string' => 'a string',
                'buffer' => "Explicit binary data",
                'true' => 1,
                'false' => nil,
                'byte' => 97,
                'array' => [ "Test", "Test3", 6, 2, OrientDbClient::Rid.new("#5:1") ],
                'date_from_time' => Time.at(1339560000).utc,
                'date_from_string' => "2012-6-13",
                'doc' => { 'integer' => 735 },
                'float' => 5.6234,
                'double' => 2.67234235,
                'bignum' => 1,
                'bigdec' => 6.2724522625234,
                'map' => { :key1 => "Value 1", "key_2" => "Value 2", "key_3" => 6234 },
                'rid' => "#3:2",
                'short' => 5,
                'time' => "2012-1-24 15:00:74",
                'implicit_embedded_documents' => [
                    { :structure => { 'null_rid' => :rid }, :document => { 'null_rid' => nil } },
                    { :structure => { 'short' => :short }, :document => { 'short' => 16134 } }
                ]
            }
        }

        expected_result = %Q{OClass@implicit_boolean_true:true,implicit_boolean_false:false,implicit_collection:[3,7,14],implicit_date:1333425600a,implicit_document:(sym_key:\"embedded doc string\"),implicit_double:6.6236d,implicit_integer:15,implicit_long:7345723467317884556l,implicit_map:{\"key1\":\"value1\",\"key2\":\"value2\"},implicit_time:1296279468t,implicit_string:\"a string\",buffer:_RXhwbGljaXQgYmluYXJ5IGRhdGE=_,true:true,false:false,byte:97b,array:[\"Test\",\"Test3\",6,2,#5:1],date_from_time:1339560000a,date_from_string:1339560000a,doc:(integer:735),float:5.6234f,double:2.67234235d,bignum:1l,bigdec:6.2724522625234c,map:{\"key1\":\"Value 1\",\"key_2\":\"Value 2\",\"key_3\":6234},rid:#3:2,short:5s,time:1327417259t,implicit_embedded_documents:[(null_rid:#),(short:16134s)]}

        result = @serializer.serialize(record)

        assert_equal expected_result, result
    end
end
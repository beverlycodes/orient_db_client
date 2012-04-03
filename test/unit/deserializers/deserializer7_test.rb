require File.join File.dirname(__FILE__), '..', '..', 'test_helper'

class TestDeserializer7 < MiniTest::Unit::TestCase
    def setup
        @deserializer = OrientDbClient::Protocols::Protocol7.deserializer
    end

    def test_deserialize
        record = %Q{ORole@name:"ORole",id:0,defaultClusterId:3,clusterIds:[3],inheritedRole:#3:2,lastUpdate:1296279468t,buffer:_U2VuZCByZWluZm9yY2VtZW50cw==_,byte:97b,date:1306274400a,float:5.6234f,double:2.67234235d,bignum:6871947673673457345l,bigdec:6.2724522625234c,properties:[(name:"mode",type:17,offset:0,mandatory:false,notNull:false,min:,max:,linkedClass:,linkedType:,index:#),*name:"rules",type:12,offset:1,mandatory:false,notNull:false,min:,max:,linkedClass:,linkedType:17,index:#*]}

        result = @deserializer.deserialize(record)

        puts y(result)

        refute_nil result
        
        assert_equal 'ORole',   result[:class]

        refute_nil  result[:structure]
        refute_nil  result[:structure]['byte']

        result[:document].tap do |d|
            assert_equal 0,         d['id']
            assert_equal 3,         d['defaultClusterId']

            assert d['inheritedRole'].is_a?(OrientDbClient::Rid), "expected Rid, but got #{d.class}"
            assert_equal '#3:2', d['inheritedRole'].to_s

            assert_equal 5.6234, d['float']
            assert_equal 2.67234235, d['double']
            assert_equal 6.2724522625234, d['bigdec']

            assert_equal "Send reinforcements", d['buffer']

            d['byte'].tap do |f|
                assert_equal 97, f
            end

            d['bignum'].tap do |f|
                assert f.is_a?(Bignum), "expected Bignum, but got #{f.class}"
                assert_equal 6871947673673457345, f
            end

            d['date'].tap do |f|
                assert f.is_a?(Date), "expected Date, but got #{f.class}"
                assert_equal 2011,  f.year
                assert_equal 5,     f.month
                assert_equal 24,    f.day
            end

            d['lastUpdate'].tap do |f|
                assert f.is_a?(Time), "expected Time, but got #{f.class}"
                assert_equal 2011,  f.year
                assert_equal 1,     f.month
                assert_equal 29,    f.day
                assert_equal 5,     f.hour
                assert_equal 37,    f.min
                assert_equal 48,    f.sec
            end

            d['clusterIds'].tap do |f|
                assert f.is_a?(Array), "expected Array, but got #{f.class}"
                assert_equal 1, f.length
                assert_equal 3, f[0]
            end

            d['properties'].tap do |f|
                assert f.is_a?(Array), "expected Array, but got #{f.class}"
                assert_equal 2, f.length

                f[0][:document].tap do |p|
                    assert_equal 'mode',    p['name']
                    assert_equal 17,        p['type']
                    assert_equal 0,         p['offset']
                    assert_equal false,     p['mandatory']
                    assert_equal false,     p['notNull']
                    assert_nil              p['min']
                    assert_nil              p['max']
                    assert_nil              p['linkedClass']
                    assert_nil              p['linkedType']
                    assert_nil              p['index']
                end

                f[1][:document].tap do |p|
                    assert_equal 'rules',   p['name']
                    assert_equal 12,        p['type']
                    assert_equal 1,         p['offset']
                    assert_equal false,     p['mandatory']
                    assert_equal false,     p['notNull']
                    assert_nil              p['min']
                    assert_nil              p['max']
                    assert_nil              p['linkedClass']
                    assert_equal 17,        p['linkedType']
                    assert_nil              p['index']
                end
            end
        end
    end

    def test_deserialize_with_maps
        record = %Q{ORole@name:"reader",inheritedRole:,mode:0,rules:{"database":null,"database.cluster.internal":2,"database.cluster.orole":3,"database.cluster.ouser":4,"database.class.*":5,"database.cluster.*":6,"database.query":7,"database.command":8,"database.hook.record":9}}

        result = @deserializer.deserialize(record)

        refute_nil result
        
        assert_equal 'ORole',   result[:class]

        result[:document].tap do |d|
            assert_nil d['inheritedRole']

            assert_equal 0, d['mode']

            d['rules'].tap do |f|
                assert f.is_a?(Hash), "expected Hash, but got #{f.class}"

                assert_nil   f['database']

                assert_equal 2, f['database.cluster.internal']
                assert_equal 3, f['database.cluster.orole']
                assert_equal 4, f['database.cluster.ouser']
                assert_equal 5, f['database.class.*']
                assert_equal 6, f['database.cluster.*']
                assert_equal 7, f['database.query']
                assert_equal 8, f['database.command']
                assert_equal 9, f['database.hook.record']
            end
        end
    end
end
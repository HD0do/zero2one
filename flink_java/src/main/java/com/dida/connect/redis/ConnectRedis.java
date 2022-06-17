package com.dida.connect.redis;

import com.alibaba.fastjson.JSONObject;
import com.dida.connect.redis.common.config.FlinkJedisPoolConfig;
import com.dida.connect.redis.common.mapper.RedisCommand;
import com.dida.connect.redis.common.mapper.RedisCommandDescription;
import com.dida.connect.redis.common.mapper.RedisMapper;
import com.dida.utils.cdc.series;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @Author：zhd
 * @Date: 2022/6/15 19:54
 * @Dscription:
 */
public class ConnectRedis {
    public static void main(String[] args) throws Exception {

        MySqlSource<String> mysql = MySqlSource.<String>builder()
                .hostname("10.150.20.11")
                .port(3306)
                .databaseList("member_db") // set captured database
                .tableList("member_db.m_member_tenant") // set captured table
                .username("readonly")
                .password("4Md0IGoBE6")
//                .startupOptions(StartupOptions.timestamp(1610208000000L))
                .deserializer(new JsonDebeziumDeserializationSchema())
                .deserializer(series.getStrSerie())
                .startupOptions(StartupOptions.initial())
                .build();



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        DataStreamSource<String> aa = env.fromSource(mysql, WatermarkStrategy.noWatermarks(), "aa");

        SingleOutputStreamOperator<JSONObject> JSONOBj = aa.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSONObject.parseObject(value);
            }
        });


        JSONOBj.print();


        // 连接到Redis的配置
        FlinkJedisPoolConfig redisConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("10.150.60.5")
                .setPort(30079)
                .setPassword("bigdata1234")
                .setMaxTotal(100)
                .setTimeout(1000 * 10)
                .build();


        JSONOBj
                .addSink(new RedisSink<>(redisConfig, new RedisMapper<JSONObject>() {
                    @Override
                    public RedisCommandDescription getCommandDescription() {
                        return new RedisCommandDescription(RedisCommand.HSET,"d");
                    }

                    @Override
                    public String getKeyFromData(JSONObject data) {

                        return "base";
                    }

                    @Override
                    public int getSecondsFromData(JSONObject data) {
                        return 0;
                    }

                    @Override
                    public String getValueFromData(JSONObject data) {
                        return data.getString("data");
                    }

                    @Override
                    public String getAdditionalKey(JSONObject data) {
                       String  hsetKey = "bigdata:data-service:member:"+data.getJSONObject("data").getString("tenant_id")+":"+data.getJSONObject("data").getString("member_id");
                        return hsetKey;
                    }
                }));


        env.execute();
    }
}

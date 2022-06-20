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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

/**
 * @Author：zhd
 * @Date: 2022/6/15 19:54
 * @Dscription:
 */
public class ConnectRedis {
    public static void main(String[] args) throws Exception {

        //获取传入参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        //环境变量获取
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);

        //获取配置
        env.getConfig().setGlobalJobParameters(parameterTool);
        ExecutionConfig.GlobalJobParameters globalJobParameters = env.getConfig().getGlobalJobParameters();
        Map<String, String> map = globalJobParameters.toMap();

        String mysqlHost = map.get("mysqlHost");
        int mysqlPort =Integer.getInteger(map.get("mysqlPort")) ;
        String mysqlPassword = map.get("mysqlPassword");
        String mysqlUser = map.get("mysqlUser");
        String redisPassword = map.get("redisPassword");
        String redisHost    = map.get("redisHost");
        String redisPort    = map.get("redisPort");


        MySqlSource<String> mysql = MySqlSource.<String>builder()
                .hostname(mysqlHost)
                .port(mysqlPort)
                .databaseList("member_db") // set captured database
                .tableList("member_db.m_member_tenant") // set captured table
                .username("readonly")
                .password("4Md0IGoBE6")
//                .startupOptions(StartupOptions.timestamp(1610208000000L))
                .deserializer(new JsonDebeziumDeserializationSchema())
                .deserializer(series.getStrSerie())
                .startupOptions(StartupOptions.initial())
                .build();

        //读取数据源
        DataStreamSource<String> aa = env.fromSource(mysql, WatermarkStrategy.noWatermarks(), "aa");




        SingleOutputStreamOperator<JSONObject> JSONOBj = aa.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSONObject.parseObject(value);
            }
        });

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

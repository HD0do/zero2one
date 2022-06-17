package com.dida.realtime.deal;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dida.realtime.pojo.UserLog;
import com.dida.utils.kafka.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * @Author：zhd
 * @Date: 2021/12/23 20:30
 * @Dscription: 实时概览实现
 */
public class OverView {
    public static void main(String[] args) throws Exception {

        //env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);

        String topic = "bigDataSensorAnalyse";
        String groupId="userLog";
        //sourcekafkaUserlog
        DataStreamSource<String> userLog = env.addSource(KafkaUtil.getKafkaSource(topic, groupId), "userLog");

        //转为javabean
        SingleOutputStreamOperator<UserLog> useractDS = userLog.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                //过滤掉为null的租户
                JSONObject jsonObject = JSON.parseObject(s);
                String tenantid = jsonObject.getJSONObject("properties").getString("tenantid");
                return tenantid!=null;
            }
        }).map(new MapFunction<String, UserLog>() {

            @Override
            public UserLog map(String s) throws Exception {
                JSONObject json = JSON.parseObject(s);
                return new UserLog(json.getLong("time"),1L, json);
            }
        });

        //定义watermark
        WatermarkStrategy<UserLog> wms = WatermarkStrategy.<UserLog>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<UserLog>() {
            @Override
            public long extractTimestamp(UserLog userLog, long timestamp) {
                return userLog.getTimestamp();
            }
        });

        //使用watermark
        useractDS
                .assignTimestampsAndWatermarks(wms)
                .keyBy(userLog1 -> userLog1.getEvent().getJSONObject("properties").getString("tenantid"))
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
               .reduce(new ReduceFunction<UserLog>() {
                   @Override
                   public UserLog reduce(UserLog userLog2, UserLog userLog1) throws Exception {
                       userLog1.setPv(userLog1.getPv()+userLog2.getPv());
                       return userLog1;
                   }
               })
                .print(">>>");

        env.execute();
    }
}

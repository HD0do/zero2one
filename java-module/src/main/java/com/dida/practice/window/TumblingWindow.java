package com.dida.practice.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Tuple;

import java.util.Arrays;

/**
 * @Author：zhd
 * @Date: 2023/3/3 17:57
 * @Dscription: 滚动窗口练习
 */
public class TumblingWindow {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("10.150.60.2",9999)
                .flatMap(new FlatMapFunction<String, Tuple3<String,Long,Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple3<String, Long,Long>> out) throws Exception {
                        Arrays.stream(value.split(","))
                                .forEach(x -> out.collect(Tuple3.of(x,1L,1L)));
                    }
                })
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(8)))
                .sum(1)
                .print();




        env.execute();
    }
}

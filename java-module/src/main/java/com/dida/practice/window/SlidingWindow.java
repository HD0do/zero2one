package com.dida.practice.window;

import com.mysql.cj.xdevapi.TableImpl;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


import java.util.Arrays;

/**
 * @Author：zhd
 * @Date: 2023/3/6 10:29
 * @Dscription: 滑动窗口练习
 */
public class SlidingWindow {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.socketTextStream("10.150.60.2",9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        Arrays.stream(value.split(","))
                                .forEach(x -> out.collect(Tuple2.of(x,1L)));
                    }
                })
                .keyBy(t -> t.f0)
                //一分钟的窗口，5秒一滑动
                .window(SlidingProcessingTimeWindows.of(Time.minutes(1),Time.seconds(5)))
                .sum(1)
                .print();

        env.execute();

    }
}

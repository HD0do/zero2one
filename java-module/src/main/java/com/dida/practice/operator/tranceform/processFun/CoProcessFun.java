package com.dida.practice.operator.tranceform.processFun;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Optional;

/**
 * @Author：zhd
 * @Date: 2023/3/6 16:57
 * @Dscription:
 */
public class CoProcessFun {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        DataStreamSource<Integer> intDataStream = env.fromElements(1, 2, 3, 4, 5, 6);

        DataStreamSource<String> strdataStream = env.fromElements("a", "b", "c", "d","e");

        intDataStream.connect(strdataStream)
                .process(new CoProcessFunction<Integer, String, String>() {
                    @Override
                    public void processElement1(Integer value, Context ctx, Collector<String> out) throws Exception {
                        out.collect(value.toString());
                    }

                    @Override
                    public void processElement2(String value, Context ctx, Collector<String> out) throws Exception {
                        out.collect(value + "：我被处理了");
                    }
                })
                .print();


        env.execute();


    }
}

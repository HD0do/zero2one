package com.dida.practice.operator.simpleCase;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


import java.util.Arrays;

/**
 * @Author：zhd
 * @Date: 2023/3/7 14:20
 * @Dscription:
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.socketTextStream("10.150.60.2",9999)
                .flatMap((String line , Collector<String> out) -> {
                    Arrays.stream(line.split(","))
                            .forEach(word -> out.collect(word));
                })
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word,1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t->t.f0)
                //内部匿名函数
//                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
//                    @Override
//                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
//                        return Tuple2.of(value1.f0,value1.f1+ value2.f1);
//                    }
//                })
                //lambda 表达式
                .reduce((value1,value2) -> {
                    return Tuple2.of(value1.f0,value1.f1+value2.f1);
                })
//                .sum(1)
                .print();

        env.execute("wordCount");
    }
}

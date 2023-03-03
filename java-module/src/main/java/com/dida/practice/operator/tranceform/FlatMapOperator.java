package com.dida.practice.operator.tranceform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author：zhd
 * @Date: 2023/2/28 15:50
 * @Dscription: flatmap算子，将数据一边多，变胖展开
 */
public class FlatMapOperator {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置平行度
        env.setParallelism(1);
        //设置状态保存

        env.fromElements("1213,fdsg","fadgdas,fdsga,aeta,gadsg","1213,fdsg")
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        for ( String word :value.split(",")){
                            out.collect(word);
                        }
                    }
                })
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        return value + ":" + "mabey";
                    }
                })
                .keyBy(new KeySelector<String, Object>() {
                    @Override
                    public Object getKey(String value) throws Exception {
                        return value;
                    }
                })
                .reduce(new ReduceFunction<String>() {
                    @Override
                    public String reduce(String value1, String value2) throws Exception {
                        return value1+value2;
                    }
                })
                .print();


        env.execute();
    }
}

package com.dida.practice.operator.tranceform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author：zhd
 * @Date: 2023/3/1 13:47
 * @Dscription: filter算子
 */
public class FilterOperator {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(1,2,3,45,6,7)
                .filter(new FilterFunction<Integer>() {
                    @Override
                    public boolean filter(Integer value) throws Exception {
                        //为真则通过，false不通过
                        return value > 1;
                    }
                })
                .print();

        env.execute();


    }
}

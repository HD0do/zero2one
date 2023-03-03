package com.dida.practice.operator.tranceform;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author：zhd
 * @Date: 2023/3/1 13:57
 * @Dscription:
 */
public class KeyByOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(1,2,3,45,6,7,6)
                .keyBy(new KeySelector<Integer, String>() {
                    @Override
                    public String getKey(Integer value) throws Exception {
                        return value % 2 ==0 ? "偶数" : "奇数";
                    }
                })
                .print();

        //执行
        env.execute();

    }
}

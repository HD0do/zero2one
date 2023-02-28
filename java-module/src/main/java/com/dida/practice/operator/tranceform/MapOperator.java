package com.dida.practice.operator.tranceform;



import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author：zhd
 * @Date: 2023/2/28 15:00
 * @Dscription: transtorm中map算子使用
 */
public class MapOperator {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(1,2,3,4,5)
             //lambda    实现
           .map(value -> value*2)

           //匿名内部类对象实现
//           .map(new MapFunction<Integer, Object>() {
//               @Override
//               public Object map(Integer value) throws Exception {
//                   return value*2;
//               }
//           })
           .print();

        env.execute();
    }

}

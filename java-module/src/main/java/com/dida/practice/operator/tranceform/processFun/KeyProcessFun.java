package com.dida.practice.operator.tranceform.processFun;

import com.dida.practice.pojo.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author：zhd
 * @Date: 2023/3/6 13:49
 * @Dscription: keyprocessfunction
 */
public class KeyProcessFun {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("10.150.60.2",9999)
                .map(line -> {
                    String[] datas = line.split(",");
                    return new WaterSensor(datas[0],Long.valueOf(datas[1]),Integer.valueOf(datas[2]));
                })
                .keyBy(wt -> wt.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, Long>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<Long> out) throws Exception {
                        System.out.println(ctx.getCurrentKey());
                        out.collect(value.getTs());
                    }
                })
                .print();

        env.execute();
    }
}

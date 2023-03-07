package com.dida.practice.operator.tranceform.processFun;

import com.dida.practice.pojo.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

/**
 * @Author：zhd
 * @Date: 2023/3/6 11:35
 * @Dscription:
 */
public class ProcessFun {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度设计
        env.setParallelism(1);

        //接收数据流
        env.socketTextStream("10.150.60.2",9999)
                .map(line -> {
                    String[] datas = line.split(",");
                    return new WaterSensor(datas[0],Long.valueOf(datas[1]),Integer.valueOf(datas[2]));
                })
                .process(new ProcessFunction<WaterSensor, Long>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<Long> out) throws Exception {
                        out.collect(value.getTs());
                    }
                })
                .print();



        //执行
        env.execute();
    }
}

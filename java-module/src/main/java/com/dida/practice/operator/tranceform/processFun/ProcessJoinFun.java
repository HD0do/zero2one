package com.dida.practice.operator.tranceform.processFun;

import com.dida.practice.pojo.WaterSensor;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Author：zhd
 * @Date: 2023/3/13 17:25
 * @Dscription:
 */
public class ProcessJoinFun {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<WaterSensor> s1 = env.socketTextStream("10.150.60.2", 9999)
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                });

        SingleOutputStreamOperator<WaterSensor> s2 = env.socketTextStream("10.150.60.2", 9999)
                .map(value -> {
                    String[] datas = value.split(",");
                    return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                });

        s1.join(s2)
                //第一个数据流的关联字段
                .where(WaterSensor::getId)
                //第二个数据流
                .equalTo(WaterSensor::getId)
                //必须开窗
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                //TODO 处理关联后数据
                .apply(new JoinFunction<WaterSensor, WaterSensor, String>() {
                    @Override
                    public String join(WaterSensor first, WaterSensor second) throws Exception {
                        return first + " ：" +second;
                    }
                })
                .print();


        env.execute();

    }
}

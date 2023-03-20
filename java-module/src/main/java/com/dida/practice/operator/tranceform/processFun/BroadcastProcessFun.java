package com.dida.practice.operator.tranceform.processFun;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author：zhd
 * @Date: 2023/3/14 17:56
 * @Dscription:
 */
public class BroadcastProcessFun {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.socketTextStream("10.150.60.2", 9999);
        DataStreamSource<String> controlStream = env.socketTextStream("10.150.60.2", 8888);

        MapStateDescriptor<String, String> stateDescriptor = new MapStateDescriptor<>("state", String.class, String.class);

        BroadcastStream<String> broadcastStream = controlStream.broadcast(stateDescriptor);

        dataStream.connect(broadcastStream)
                .process(new BroadcastProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {


                    }

                    @Override
                    public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {

                    }
                }).print();


        env.execute();

    }
}

package com.dida.practice.operator.tranceform;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author：zhd
 * @Date: 2023/3/3 16:14
 * @Dscription:
 */
public class ConnectOperator {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4, 56, 7);
        DataStreamSource<String> stringDataStreamSource = env.fromElements("a", "b", "c", "d");

        ConnectedStreams<Integer, String> cs = integerDataStreamSource.connect(stringDataStreamSource);

        cs.getFirstInput()
                .print("int");

        cs.getSecondInput()
                .print("string");

        env.execute();

    }
}

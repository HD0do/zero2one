package com.dida.filesystem.s3;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author：zhd
 * @Date: 2022/5/12 21:42
 * @Dscription: 测试连接minio
 */
public class FilesystemS3 {


//test

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.readTextFile("s3a://flink/checkpoint");

        text.print();


        env.execute();

//http://10.150.60.10:31001
    }


}

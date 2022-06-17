package com.dida.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class Source_Kafka {
    public static void main(String[] args) throws Exception {
        //0.kafka 相关配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.150.20.12:9092");
        properties.setProperty("group.id", "Source_Kafka");
        properties.setProperty("auto.offset.reset", "latest");
        //1.配置环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取kafka消息
        env.addSource(new FlinkKafkaConsumer<String>("bigDataSensorAnalyse",new SimpleStringSchema(),properties)).print();
        //执行任务
        env.execute();

    }

}

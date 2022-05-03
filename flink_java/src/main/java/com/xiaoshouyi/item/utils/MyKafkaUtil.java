package com.xiaoshouyi.item.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @Author：zhd
 * @Date: 2021/8/19 20:01
 * @Dscription:
 */

public class MyKafkaUtil {
    //准备信息
    private static Properties properties = new Properties();

    //kafka连接参数据
    private static final String KAFKA_SERVER = "10.150.20.12:9092";

    static{
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
    }

    /**
     *
     * //消费kafka的消费者
     *
     * @param groupId 消费者组
     * @param topic  kafka主题名字
     * @return kafkaConsumerFun
     */
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic,String groupId){

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"");

        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),properties);
    }

    /**
     *
     * kafka的生产者
     *
     * @param topic
     * @return
     */
    public static FlinkKafkaProducer<String> getKafkaSink (String topic) {
        return new FlinkKafkaProducer<String>(topic,new SimpleStringSchema(),properties);
    }

    /**
     * 当使用tableAPI时候这个是通用的部分，创建消费kafka主题的配置
     *
     * @param groupId
     * @param topic
     * @return
     */
    public static String getKafkaDDL(String groupId,String topic){
        return "'connector' = 'kafka', "+
                "'topic' = "+topic + ","+
                "'properties.bootstrap.servers' = '" + KAFKA_SERVER + "',"+
                "'format' = 'json' "+
                "'scan.startup.mode' = 'latest-offset' ";
    }
}

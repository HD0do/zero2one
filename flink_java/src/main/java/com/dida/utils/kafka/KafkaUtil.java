package com.dida.utils.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @Author：zhd
 * @Date: 2021/12/23 20:04
 * @Dscription: kafka->source.sink util实现
 */
public class KafkaUtil {

    private static Properties properties = new Properties();
    //kafka的的地址配置
    private static final String kafkaUrl = "10.150.20.12:9092";
    //放入配置
    static {
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaUrl);
    }


    /**
     *
     * @param topic :kafka主题
     * @param groupID：消费者组
     * @return ：kafkaconsumer
     */
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic,String groupID){
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupID);
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),properties);

    }

    public static String getKafkaSqlDDL (String topic,String groupId){
        String ddl="('connector' = 'kafka', " +
                " 'topic' = '"+topic+"',"   +
                " 'properties.bootstrap.servers' = '"+ kafkaUrl +"', " +
                " 'properties.group.id' = '"+groupId+ "', " +
                "  'format' = 'json', " +
                "  'scan.startup.mode' = 'latest-offset')";

        System.out.println(ddl);
        return  ddl;
    }


}

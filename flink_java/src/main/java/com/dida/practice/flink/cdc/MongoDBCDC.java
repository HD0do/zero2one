package com.dida.practice.flink.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mongodb.MongoDBSource;


/**
 * @Author：zhd
 * @Date: 2021/11/24 9:50
 * @Dscription:
 */
public class MongoDBCDC {

    public static void main(String[] args) throws Exception {
        SourceFunction<String> sourceFunction = MongoDBSource.<String>builder()
                .hosts("10.150.10.12:27017")
                .username("readonly")
                .password("75VPIbqHdE")
                .database("promotion_db")
//                .collection("promotion_activity")
                .collection("coupon_ticket")
//                .database("order_db")
//                .collection("pos_shift_record")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(sourceFunction)
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute();
    }
}

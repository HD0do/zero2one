package com.xiaoshouyi.practice.flink.cdc;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public class MySQLCDC {
    public static void main(String[] args) throws Exception {

        MySqlSource<String> mysql = MySqlSource.<String>builder()
                .hostname("10.150.20.11")
                .port(3306)
                .databaseList("order_db") // set captured database
                .tableList("order_db.order_header") // set captured table
                .username("readonly")
                .password("4Md0IGoBE6")
                .startupOptions(StartupOptions.timestamp(1610208000000L))
                .deserializer(new JsonDebeziumDeserializationSchema())
//                .deserializer(new DebeziumDeserializationSchema<String>() {
//                                  @Override
//                                  public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
//                                      //获取主题信息,包含着数据库和表名  mysql_binlog_source.gmall-flink.z_user_info
//                                      String topic = sourceRecord.topic();
//                                      String[] arr = topic.split("\\.");
//                                      String db = arr[1];
//                                      String tableName = arr[2];
//
//                                      //获取操作类型 READ DELETE UPDATE CREATE
//                                      Envelope.Operation operation = Envelope.operationFor(sourceRecord);
//
//                                      //获取值信息并转换为Struct类型
//                                      Struct value = (Struct) sourceRecord.value();
//
//                                      //获取变化后的数据
//                                      Struct after = value.getStruct("after");
//
//                                      //创建JSON对象用于存储数据信息
//                                      JSONObject data = new JSONObject();
//                                      for (Field field : after.schema().fields()) {
//                                          Object o = after.get(field);
//                                          data.put(field.name(), o);
//                                      }
//
//                                      //创建JSON对象用于封装最终返回值数据信息
//                                      JSONObject result = new JSONObject();
//                                      result.put("operation", operation.toString().toLowerCase());
//                                      result.put("data", data);
//                                      result.put("database", db);
//                                      result.put("table", tableName);
//
//                                      //发送数据至下游
//                                      collector.collect(result.toJSONString());
//
//                                  }
//
//                                  @Override
//                                  public TypeInformation<String> getProducedType() {
//                                      return TypeInformation.of(String.class);
//                                  }
//                              }) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.initial())
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        //env.enableCheckpointing(3000);

//        DataStreamSource<String> aa = env.addSource(mysql);
        DataStreamSource<String> aa = env.fromSource(mysql, WatermarkStrategy.noWatermarks(), "aa");

        aa
//                .map(str->{
//            JSONObject jsonObject = JSON.parseObject(str);
//
//            long time = jsonObject.getJSONObject("data").getLong("created_at");
//
//            String result2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time);
//            return  result2;
//        }).filter(str->str.contains("2021-06-24"))
                .print();
        env.execute();
    }
}

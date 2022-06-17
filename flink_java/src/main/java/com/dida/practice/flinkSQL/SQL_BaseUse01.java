package com.dida.practice.flinkSQL;


import com.dida.pojo.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author：zhd
 * @Date: 2021/9/7 19:53
 * @Dscription:
 */

public class SQL_BaseUse01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //生成数据流
        DataStreamSource<WaterSensor> waterSensorStream = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        //创建执行SQL执行的环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Configuration configuration = tableEnv.getConfig().getConfiguration();
        configuration.setString("table.exec.mini-batch.enabled","true");
        //使用SQL查询未注册的表
        Table inputTable = tableEnv.fromDataStream(waterSensorStream);
        Table resultTable = tableEnv.sqlQuery("select id from " + inputTable + " where id='sensor_1'");

        tableEnv.toAppendStream(resultTable, Row.class).print();
        //开始执行
        env.execute();

    }
}

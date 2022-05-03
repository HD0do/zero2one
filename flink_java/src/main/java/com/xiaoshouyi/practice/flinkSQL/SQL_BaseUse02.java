package com.xiaoshouyi.practice.flinkSQL;

import com.xiaoshouyi.pojo.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author：zhd
 * @Date: 2021/9/8 9:47
 * @Dscription: 使用注册的表查询数据 flinkSQL
 */
public class SQL_BaseUse02 {
    public static void main(String[] args) throws Exception {
        //1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.1设置并行度
        env.setParallelism(1);
        //2.通过元素获取数据
        DataStreamSource<WaterSensor> waterSensorStream = env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));

        //创建table执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //使用SQL查询一个已注册的表
        //1.从流得到一个表
        Table inputTable = tableEnv.fromDataStream(waterSensorStream);
        //2.注册一个临时视图
        tableEnv.createTemporaryView("sensor",inputTable);
        //3.在临时视图查询数据，得到一个表
        Table resultTable = tableEnv.sqlQuery("select * from sensor where id='sensor_1'");

        //4.显示resultTable数据
        tableEnv.toAppendStream(resultTable, Row.class).print();

        //执行环境
        env.execute();
    }
}


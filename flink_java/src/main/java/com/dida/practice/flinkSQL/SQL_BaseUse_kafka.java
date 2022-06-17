package com.dida.practice.flinkSQL;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author： zhd
 * @Date: 2021/9/8 14:37
 * @Dscription: flinksql读取kafka 写入kafka
 */

public class SQL_BaseUse_kafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //创建表的执行环境
        StreamTableEnvironment tableEvn = StreamTableEnvironment.create(env);

        //1.注册sourceTable ：
        tableEvn.executeSql("create table header (id string,ts bigint,vc int)");


    }
}

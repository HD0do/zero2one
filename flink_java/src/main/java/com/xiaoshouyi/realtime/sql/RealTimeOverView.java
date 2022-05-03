package com.xiaoshouyi.realtime.sql;

import com.xiaoshouyi.realtime.pojo.UserLog;
import com.xiaoshouyi.realtime.pojo.UserLogSql;
import com.xiaoshouyi.realtime.utils.KafkaUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author：zhd
 * @Date: 2022/1/5 11:05
 * @Dscription:
 */
public class RealTimeOverView {
    public static void main(String[] args) throws Exception {
        //流式环境创建
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //平行度创建
        env.setParallelism(2);
        //SQL表流式执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        String ddlSql ="create table kafka_log_source (distinct_id string,properties Row(tenantid String) ) with " + KafkaUtil.getKafkaSqlDDL("bigDataSensorAnalyse","realtime");

        System.out.println(ddlSql);
        tableEnv.executeSql(ddlSql);

        tableEnv.sqlQuery("select distinct_id ,properties.tenantid from kafka_log_source").execute().print();


        env.execute();



    }
}

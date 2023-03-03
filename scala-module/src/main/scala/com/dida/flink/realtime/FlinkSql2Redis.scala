package com.dida.flink.realtime

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
  * @Author ：zhd
  * @Date: 2022/6/22 16:50
  * @Dscription:
  */
object FlinkSql2Redis {
  def main(args: Array[String]): Unit = {

    //执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //table执行环境
    //开启checkpoint
    env.enableCheckpointing(1000)

    val settings:EnvironmentSettings  = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env,settings)

    /**
      * `member_id` bigint(20) NOT NULL COMMENT '会员id',
      * `mobile` varchar(16) NOT NULL COMMENT '联系方式',
      * `tenant_id` varchar(10) NOT NULL COMMENT '商户号',
      */

    var mysqlLogSql:String =
      s"""
         |CREATE TABLE member_ms (
         |  `member_id`                  bigint,
         |   mobile string,
         |  `tenant_id`           string   ,
         |  gender                int ,
         |   proctime as procTime(),
         |     PRIMARY KEY(member_id) NOT ENFORCED
         |     ) WITH (
         |     'connector' = 'mysql-cdc',
         |     'hostname' = '10.150.20.11',
         |     'port' = '3306',
         |     'username' = 'readonly',
         |     'password' = '4Md0IGoBE6',
         |     'database-name' = 'member_db',
         |     'table-name' = 'm_member_tenant',
         |	 'scan.startup.mode'='latest-offset',
         |	 'debezium.skipped.operations'='d'
         |	 )
       """.stripMargin

    val result = tableEnv.executeSql(mysqlLogSql)
    //测试kafka数据源
//        tableEnv.sqlQuery("select * from member_ms").execute().print()



     var reidsSinkSql:String =
    s"""
       |CREATE TABLE memberRedis (
       |  `member_id`           bigint,
       |   mobile               string,
       |  `tenant_id`           string,
       |    gender                int
       |     ) WITH (
       |     'connector' = 'redis',
       |     'host' = '10.150.60.5',
       |     'port' = '30079',
       |     'password' = 'bigdata1234',
       |     'redis-mode' = 'single',
       |	  'command'='hset'
       |	 )
       """.stripMargin

    tableEnv.executeSql(reidsSinkSql)
    var reidsSourceSql:String =
      s"""
         |CREATE TABLE memberSourceRedis (
         |  `member_id`           bigint,
         |   mobile               string,
         |  `tenant_id`           string
         |     ) WITH (
         |     'connector' = 'redis',
         |     'host' = '10.150.60.5',
         |     'port' = '30079',
         |     'password' = 'bigdata1234',
         |     'redis-mode' = 'single',
         |	  'command'='hget',
         |   'maxIdle'='2',
         |   'minIdle'='1',
         |  'lookup.cache.max-rows'='10',
         |  'lookup.cache.ttl'='10',
         |   'lookup.max-retries'='3'
         |	 )
       """.stripMargin

    tableEnv.executeSql(reidsSourceSql)



    var insertSql:String =
      """
        |select
        |t1.member_id,
        |t1.mobile,
        |JSON_OBJECT('tenant_id' value cast(t1.tenant_id as string),'gender' value cast(t1.gender as string) )
        |from member_ms t1
      """.stripMargin


//    tableEnv.executeSql(insertSql)

    tableEnv.sqlQuery(insertSql).execute().print()

    env.execute()

  }


}

package realtime

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
  * @Author ：zhd
  * @Date: 2022/1/13 19:15
  * @Dscription:
  */
object UserLogBItMapTest {
  def main(args: Array[String]): Unit = {

    //创建流式执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env,settings)

    //批处理环境构建
    val batchSettings = EnvironmentSettings.newInstance()
      .inBatchMode()
      .useBlinkPlanner()
      .build()
    batchSettings
    val batchTableEvn: TableEnvironment = TableEnvironment.create(batchSettings)


    var kafkaLogSql:String =
      s"""create table kafka_log_source (distinct_id string,
         |properties Row(tenantid String,shop_id String,shop_group_1_id String,shop_group_2_id String),
         |event String,
         |`time` bigint,
         |dt as to_DATE(FROM_UNIXTIME(`time`/1000,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd'),
         |m as SUBSTRING(FROM_UNIXTIME(`time`/1000,'yyyy-MM-dd HH:mm:ss'),0,16),
         |h as HOUR(to_timeStamp(FROM_UNIXTIME(`time`/1000,'yyyy-MM-dd HH:mm:ss'))),
         |rowtime as TO_TIMESTAMP(FROM_UNIXTIME(`time`/1000,'yyyy-MM-dd HH:mm:ss')),
         |watermark for rowtime as rowtime - interval '5' second
         |)with ('connector' = 'kafka',
         |'topic' = 'bigDataSensorAnalyse',
         |'properties.bootstrap.servers' = '10.150.20.12:9092',
         |'properties.group.id' = 'bitmapTest',
         |'format' = 'json',
         |'scan.startup.mode' = 'latest-offset')
         |""".stripMargin

   var ckBitMapTest:String =
     """
       |create table test(
       |tenant_id String,
       |shop_id String,
       |uv string,
       |h int,
       |dt date,
       |primary key(tenant_id) not enforced
       |)with(
       |'connector'='clickhouse',
       |'url'='clickhouse://10.150.60.8:8123',
       |'username'='ck',
       |'database-name'='test',
       |'password'='bigdata1234',
       |'table-name'='bitmap_test1'
       |)
     """.stripMargin

    tableEnv.executeSql(kafkaLogSql)
    tableEnv.executeSql(ckBitMapTest)

   var uvSql:String=
      """
        |insert into test
        |select
        |tenantid as tenant_id,
        |shop_id ,
        |distinct_id as uv,cast (h as int),dt
        |from kafka_log_source
      """.stripMargin

    tableEnv.executeSql(uvSql)
//    tableEnv.sqlQuery(uvSql).execute().print()

//    env.execute()
  }
}
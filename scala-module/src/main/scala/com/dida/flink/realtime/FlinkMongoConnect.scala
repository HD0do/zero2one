package com.dida.flink.realtime

import com.ververica.cdc.connectors.mongodb.MongoDBSource
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
  * @Author ：zhd
  * @Date: 2022/6/27 17:07
  * @Dscription:
  */
object FlinkMongoConnect {

  def main(args: Array[String]): Unit = {

    var env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    var tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)


    var mongoDBKafaSql:String =
      """
        |create table members(
        |_id bigint,
        |_class string,
        |createdAt date,
        |createdBy string,
        |enable int,
        |grade int,
        |gradeRightList ROW<consumeDiscountRight ROW<discount STRING ,productScopeList ARRAY<STRING>>, gradeRightType STRING, creditMultRight STRING ,postageRight ROW<consumeAmount STRING,discountPostageAmt STRING ,postageType int>>,
        |growth String,
        |lastUpdatedAt date,
        |name string,
        |tenantId string,
        |updateBy string,
        |PRIMARY KEY(_id) NOT ENFORCED
        |)with(
        |'connector' = 'mongodb-cdc',
        |'hosts' = '10.150.20.3:27017',
        |'username' = 'readonly',
        |'password' = 'y5Gi2BjbK3',
        |'copy.existing'='true',
        |'database' = 'member_db',
        |'collection' = 'm_grade_setting'
        |)
      """.stripMargin

    tableEnv.executeSql(mongoDBKafaSql)
    tableEnv.sqlQuery("select * from members").execute().print()


    env.execute()

  }
}

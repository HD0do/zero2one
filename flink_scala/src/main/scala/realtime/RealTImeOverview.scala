package realtime

import java.sql.{Date, PreparedStatement, Timestamp}

import org.apache.flink.api.scala._
import org.apache.flink.connector.jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcExecutionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment, TableResult}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
  * @Author ：zhd
  * @Date: 2022/1/5 13:47
  * @Dscription:
  */
object RealTImeOverview {
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
         |'properties.group.id' = 'realtime1  ',
         |'format' = 'json',
         |'scan.startup.mode' = 'latest-offset')
         |""".stripMargin

    println("------------------------kafka_log_source DDL SQL------------------------------")
    println(kafkaLogSql)


   var orderSql:String =
      s"""
        |create table order_source_ms(id BIGINT,deal_amt DOUBLE,shop_id STRING,customer_id String,city_id bigint,product_count double,
        |order_at timestamp(3),last_updated_at timestamp(3),pay_at timestamp,refund_at timestamp,
        |tenant_id STRING,order_category STRING,
        |h as hour(last_updated_at),
        |pay_hour as hour(pay_at),
        |refund_hour as hour(refund_at),
        |m as MINUTE(last_updated_at),
        |dt as to_DATE(cast(last_updated_at as string)),
        |pay_dt as to_DATE(cast(pay_at as string)),
        |refund_dt as to_DATE(cast(refund_at as string)),
        |PRIMARY KEY(id) NOT ENFORCED)
        |with(
        |'connector' ='mysql-cdc',
        |'hostname' ='10.150.20.11',
        |'port'='3306',
        |'username' = 'readonly',
        |'password' = '4Md0IGoBE6',
        |'database-name'='order_db',
        |'scan.startup.mode'='latest-offset',
        |'table-name'='order_header')
      """.stripMargin
    print("--------------------------------------order_source_ms DDL SQL------------------------------------------")
    println(orderSql)

    var shopDim:String =
      """
        |create table shop_dim(id BIGINT,shop_id String,
        |PRIMARY KEY (id) NOT ENFORCED)
        |with(
        |'connector' ='jdbc',
        |'url'='jdbc:mysql://10.150.20.11:3306/product_db',
        |'table-name'='p_shop',
        |'username'='readonly',
        |'password'='4Md0IGoBE6')
      """.stripMargin
    println("------------------------------------shopDim DDL SQL ------------------------------------------------")
    println(shopDim)

    print("--------------------------------------ckTestSink  DDL  SQL---------------------------------------------")

    var ckTestSink:String =
      """
        |create table test(
        |tenant_id String,
        |shop_id String,
        |pv bigint,
        |uv bigint,
        |pay_pcnt bigint,
        |pay_amt double,
        |pay_qty double,
        |pay_cnt bigint,
        |refund_amt double,
        |refund_cnt bigint,
        |recruit_qty bigint,
        |write_time timestamp,
        |h int,
        |dt Date,
        |primary key (tenant_id) not enforced
        |)with(
        |'connector'='jdbc',
        |'url'='jdbc:clickhouse://10.150.60.8:8123/test',
        |'username'='ck',
        |'driver'='ru.yandex.clickhouse.ClickHouseDriver',
        |'password'='bigdata1234',
        |'table-name'='test'
        |)
      """.stripMargin
    println(ckTestSink)

  var  ckProductSql:String =
      """
        |create table product_deal_sink(
        |tenant_id String,
        |shop_id String,
        |pay_amt double,
        |pay_qty double,
        |sku_code String,
        |category_code_B String,
        |write_time timestamp,
        |dt Date,
        |primary key (tenant_id) not enforced
        |)with(
        |'connector'='clickhouse',
        |'url'='clickhouse://10.150.60.8:8123',
        |'username'='ck',
        |'username'='ru.yandex.clickhouse.ClickHouseDriver',
        |'password'='bigdata1234',
        |'table-name'='product_deal',
        |'database-name'='real_sh'
        |)
      """.stripMargin

 var   memberSql:String =
      """
        |create table member_ms(
        |member_id bigint,
        |tenant_id String,
        |created_at Timestamp,
        |dt as to_DATE(cast(created_at as string)),
        |h as hour(created_at),
        |primary key (member_id)not enforced
        |)with(
        |'connector' ='mysql-cdc',
        |'hostname' ='10.150.20.11',
        |'port'='3306',
        |'username' = 'readonly',
        |'password' = '4Md0IGoBE6',
        |'database-name'='member_db',
        |'scan.startup.mode'='initial',
        |'table-name'='m_member_tenant')
      """.stripMargin

    var itmeSql:String =
      s"""
         |create table item_source_ms(id BIGINT,deal_sub_amt DOUBLE,qty double,order_id bigint,sku_code string,
         |category_code string,
         |category_code_B as SUBSTRING(category_code FROM 1 FOR 2),
         |created_at timestamp(3),last_updated_at timestamp(3),
         |tenant_id STRING,
         |h as hour(last_updated_at),
         |m as MINUTE(last_updated_at),
         |dt as to_DATE(cast(last_updated_at as string)),
         |PRIMARY KEY(id) NOT ENFORCED)
         |with(
         |'connector' ='mysql-cdc',
         |'hostname' ='10.150.20.11',
         |'port'='3306',
         |'username' = 'readonly',
         |'password' = '4Md0IGoBE6',
         |'database-name'='order_db',
         |'scan.startup.mode'='latest-offset',
         |'table-name'='order_item')
      """.stripMargin
    print("--------------------------------------item_source_ms DDL SQL------------------------------------------")
    println(itmeSql)
//rowtime as last_updated_at,
    //watermark for rowtime as rowtime - interval '5' minute,


    //TODO kafka_log执行
    val result = tableEnv.executeSql(kafkaLogSql)
    //TODO orderSql执行
    val result1 = tableEnv.executeSql(orderSql)

//    tableEnv.sqlQuery("select * from order_source_ms").execute().print()
    //TODO ck的sink表创建
    val result2 = tableEnv.executeSql(ckTestSink)
    //todo shop维度表
    val resultShopDim: TableResult = tableEnv.executeSql(shopDim)
    //todo memeber_tenant
    val resultMember: TableResult = tableEnv.executeSql(memberSql)
    //todo itemSql执行
    val resultItem: TableResult = tableEnv.executeSql(itmeSql)
    //todo ckProduct商品表
    tableEnv.executeSql(ckProductSql)
    

    //访问实时数据统计
    var viewSqlWindow:String =
      """
        |select
        |DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '1' MINUTE ),'yyyy-MM-dd HH:mm:ss') stt,
        |DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '1' MINUTE ),'yyyy-MM-dd HH:mm:ss')  edt ,
        |properties.tenantid,properties.shop_id,properties.shop_group_1_id,properties.shop_group_2_id,
        |m,h,dt,
        |count (distinct distinct_id) as uv,sum(1) as pv
        |from kafka_log_source
        |where (event = 'pageShow' or event = 'detailPageView') and properties.tenantid is not null
        |group by properties.tenantid,properties.shop_id,properties.shop_group_1_id,properties.shop_group_2_id,
        |m,h,dt,TUMBLE(rowtime,INTERVAL '1' MINUTE)
      """.stripMargin
    println("-------------------------------------viewSql ------------------------------------------------------")
    println(viewSqlWindow)
    println("---------------------------------------------------------------------------------------------------")
//     tableEnv.sqlQuery(viewSqlWindow).execute().print()
//    tableEnv.sqlQuery("select distinct_id ,event,`time` as view_timestamp ,h,m,dt from kafka_log_source ").execute().print()

    var viewSql:String =
      """
        |create view view_static_tmp as
        |select
        |properties.shop_id as shop_id,properties.tenantid as tenant_id,
        |count (distinct distinct_id) as uv,
        |sum(1) as pv,
        |0 as pay_pcnt,
        |0 as pay_amt,
        |0 as pay_qty,
        |0 as pay_cnt,
        |0 as refund_amt,
        |0 as refund_cnt,
        |0 as recruit_qty,
        |h,
        |dt
        |from kafka_log_source
        |where (event = 'pageShow' or event = 'detailPageView') and properties.tenantid is not null
        |group by properties.shop_id ,properties.tenantid,dt,h
      """.stripMargin

    tableEnv.executeSql(viewSql)
//    tableEnv.sqlQuery(viewSql).execute().print()

    var dealSql:String =
      """
        |create view deal_static_tmp as
        |select
        |shop_id,tenant_id,
        |0 as uv,
        |0 as pv,
        |sum (pay_pcnt) as pay_pcnt,
        |sum(pay_amt) as pay_amt,
        |sum(pay_qty) as pay_qty,
        |sum(pay_cnt) as pay_cnt,
        |sum(refund_amt) as refund_amt,
        |sum(refund_cnt) as refund_cnt,
        | sum(recruit_qty) as recruit_qty,
        |h,
        |dt
        |from
        |(select
        |shop_id,tenant_id,
        |count (distinct customer_id) as pay_pcnt,
        |sum(deal_amt) as pay_amt,
        |sum(product_count) as pay_qty,
        |sum(1) as pay_cnt,
        |0 as refund_amt,
        |0 as refund_cnt,
        |0 as recruit_qty,
        |pay_hour as h,
        |pay_dt as dt
        |from order_source_ms
        |where pay_dt = CURRENT_DATE
        |group by shop_id,tenant_id,pay_dt,pay_hour
        |union all
        |(
        |select
        |shop_id,tenant_id,
        |0 as pay_pcnt,
        |0 as pay_amt,
        |0 as pay_qty,
        |0 as pay_cnt,
        |sum(if (order_category ='reverse', deal_amt,0)) as refund_amt,
        |sum(if (order_category ='reverse', 1,0)) as refund_cnt,
        | 0 as recruit_qty,
        |refund_hour as h,
        |refund_dt as dt
        |from order_source_ms
        |where refund_dt = CURRENT_DATE
        |group by shop_id,tenant_id,refund_dt,refund_hour
        |))
        |group by shop_id,tenant_id,dt,h
      """.stripMargin
    tableEnv.executeSql(dealSql)

//    tableEnv.sqlQuery("select * from deal_static_tmp").execute().print()

// todo deal test

//    tableEnv.sqlQuery(dealSql).execute().print()
//    var table: Table = tableEnv.sqlQuery("select deal_amt as amt,city_id,order_at,order_category,last_updated_at,pay_at from order_source_ms ")
//    table.execute().print()

    var recruitMemSQL:String =
      """
        |create view recruit_member_tmp as
        |select
        |'*'as shop_id,tenant_id,
        |0 as uv,
        |0 as pv,
        |0 as pay_pcnt,
        |0 as pay_amt,
        |0 as pay_qty,
        |0 as pay_cnt,
        |0 as refund_amt,
        |0 as refund_cnt,
        |count (distinct member_id) recruit_qty,
        |h,
        |dt
        |from member_ms
        |where CURRENT_DATE=dt
        |group by dt,tenant_id,h
      """.stripMargin
    tableEnv.executeSql(recruitMemSQL)

    //todo memberTest
//    tableEnv.sqlQuery(recruitMemSQL).execute().print()
//  memberTest
//    tableEnv.sqlQuery("select * from member_ms").execute().print()

    //todo union test
//    tableEnv.executeSql("create view a AS select tenant_id from deal_static_tmp ")
//    tableEnv.executeSql("create view b AS select tenant_id from view_static_tmp ")
//    tableEnv.sqlQuery("select * from () tenant_id from a union all b")

    //todo 商品统计
    //TODO item表和header表关联 insert into product_deal_sink
    var itemHeaderJoin:String =
      """
        |select
        |t1.tenant_id as tenant_id,
        |t1.shop_id as shop_id,
        |sum(deal_sub_amt) as pay_amt,
        |sum(qty) as pay_qty,
        |sku_code,
        |category_code_B,
        |localTimeStamp as write_time,
        |t1.dt as dt
        |from (select * from order_source_ms where pay_dt=CURRENT_DATE ) t1
        |join item_source_ms t2
        |on t1.id=t2.order_id
        |where pay_at is not null and order_category ='sale'
        |group by t1.shop_id,t1.tenant_id,sku_code,category_code_B,t1.dt
      """.stripMargin

    //todo item join header test
//    tableEnv.executeSql(itemHeaderJoin)
    tableEnv.sqlQuery(itemHeaderJoin).execute().print()

    //todo deal and view data union
    var dealViewSql:String =
      """
        |select tenant_id,shop_id ,
        |sum(pv) as pv,
        |sum(uv) as uv,
        |sum(pay_pcnt)as pay_pcnt,
        |sum(pay_amt) as pay_amt,
        |sum(pay_qty) as pay_qty,
        |sum(pay_cnt) as pay_cnt,
        |sum(refund_amt) as refund_amt,
        |sum(refund_cnt) as refund_cnt,
        |sum(recruit_qty) as recruit_qty,
        |cast (localtimestamp as string) as wirte_time,
        |h,
        |dt
        | from (
        | select * from deal_static_tmp
        | union all
        | select * from view_static_tmp
        | union all
        |select * from recruit_member_tmp )
        | group by shop_id,tenant_id,dt,h
      """.stripMargin

//    tableEnv.sqlQuery(dealViewSql).execute().print()
    val viewDealTable: Table = tableEnv.sqlQuery(dealViewSql)

    val viewDeal: DataStream[(Boolean, DataStatic)] = tableEnv.toRetractStream[DataStatic](viewDealTable).filter(_._1)
//   viewDeal.print()
    val viewDealStream: DataStream[DataStatic] = viewDeal.map(_._2)

    viewDealStream.filter(_.pv!=0).print()

    viewDealStream.filter(_.pv!=0).addSink(
      JdbcSink.sink(
        //插入数据的SQL语句
        "insert into deal_view_sh(tenant_id,shop_id,pv,uv,pay_pcnt,pay_amt,pay_qty,pay_cnt,refund_amt,refund_cnt,recruit_qty,write_time,h,dt)values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        new JdbcStatementBuilder[DataStatic] {
          override def accept(ps: PreparedStatement, t: DataStatic): Unit = {
            ps.setString(1,t.tenant_id)
            ps.setString(2,t.shop_id)
            ps.setLong(3,t.pv)
            ps.setLong(4,t.uv)
            ps.setLong(5,t.pay_pcnt)
            ps.setDouble(6,t.pay_amt)
            ps.setDouble(7,t.pay_qty)
            ps.setLong(8,t.pay_cnt)
            ps.setDouble(9,t.refund_amt)
            ps.setLong(10,t.refund_cnt)
            ps.setLong(11,t.recruit_qty)
            ps.setString(12,t.write_time)
            ps.setLong(13,t.h)
            ps.setDate(14,t.dt)
          }
        },
        //写入的参数配置
        JdbcExecutionOptions.builder()
          .withBatchSize(5)
          .withBatchIntervalMs(100)
          .build(),
        //连接参数配置
        new JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:clickhouse://10.150.60.8:8123/real_sh")
          .withUsername("ck")
          .withPassword("bigdata1234")
          .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
          .build()
      )
    )

//    tableEnv.executeSql("insert into test select tenant_id,shop_id ,sum(pay_amt) as amt,LOCALTIMESTAMP from (select * from deal_static_tmp union all  select * from view_static_tmp )group by shop_id,tenant_id")
    tableEnv.sqlQuery("select * from shop_dim").execute().print()

//    tableEnv.sqlQuery("select sum(if(order_category ='sale', deal_amt,0)),sum(1) from order_source_ms  where pay_at is not null or refund_at is not null ").execute().print()
//   tableEnv.sqlQuery("select * from ck_test_sink ").execute().print()

    env.execute("realtime")
  }

}

case class DataStatic(
        tenant_id:String,
        shop_id:String,
        pv:Long,
        uv:Long,
        pay_pcnt:Long,
        pay_amt:Double,
        pay_qty:Double,
        pay_cnt:Long,
        refund_amt:Double,
        refund_cnt:Long,
        recruit_qty:Long,
        write_time:String,
        h:Long,
        dt:Date
 )

case class ProductDataStatic(
         tenant_id:String,
         shop_id:String,
         pay_amt:Double,
         sku_code:String,
         category_code_b:String,
         write_time:String,
         dt:Date
  )

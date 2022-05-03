package test.sql

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
  * @Author ：zhd
  * @Date: 2022/1/15 15:04
  * @Dscription: 这个是用来测试flinkSQL监听mysqlcdc数据删除后的变化
  */
object SqlCDCDeleteTest {
  def main(args: Array[String]): Unit = {
    //构建流式SQL执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings = EnvironmentSettings.newInstance()
      .inStreamingMode()
      .useBlinkPlanner()
      .build()

    env.setParallelism(1)

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env,settings)


    var orderSql:String =
      s"""CREATE TABLE order_db_order_header_ms (
         |    id           bigint,
         |    tenant_id                   varchar   ,
         |    dt as to_DATE(cast(order_at as string)),
         |    serial_number               varchar   ,
         |    order_category              varchar    ,
         |    order_type                  varchar    ,
         |    order_status                varchar   ,
         |    title                       varchar   ,
         |    product_count               decimal(19, 2) ,
         |    customer_id                 varchar    ,
         |    customer_name               varchar   ,
         |    customer_phone              varchar    ,
         |    customer_type               int            ,
         |    shop_id                     varchar   ,
         |    shop_name                   varchar  ,
         |    payment_type                varchar   ,
         |    order_sub_type              varchar    ,
         |    channel_id                  int            ,
         |    deal_amt                    decimal(19, 2) ,
         |    paid_amt                    decimal(19, 2) ,
         |    p_paid_amt                  decimal(19, 2) ,
         |    f_paid_amt                  decimal(19, 2) ,
         |    p_pay_amt                   decimal(19, 2) ,
         |    f_pay_amt                   decimal(19, 2) ,
         |    p_lp_amt                    decimal(19, 2) ,
         |    p_sp_amt                    decimal(19, 2) ,
         |    total_promo_amt             decimal(19, 2) ,
         |    p_promo_amt                 decimal(19, 2) ,
         |    f_amt                       decimal(19, 2) ,
         |    f_promo_amt                 decimal(19, 2) ,
         |    p_credit_pay_amt            decimal(19, 2) ,
         |    f_credit_pay_amt            decimal(19, 2) ,
         |    p_member_store_pay_amt      decimal(19, 2) ,
         |    f_member_store_pay_amt      decimal(19, 2) ,
         |    p_cash_pay_amt              decimal(19, 2) ,
         |    p_change_amt                decimal(19, 2) ,
         |    third_party_pay_rate        decimal(9, 4)  ,
         |    third_party_pay_charge_amt  decimal(9, 2)  ,
         |    remark                      varchar   ,
         |    internal_remark             varchar   ,
         |    reason                      varchar   ,
         |    images                      varchar  ,
         |    first_buy_flag              tinyint     ,
         |    source_order_id             bigint         ,
         |    latest_transition_order_id  varchar   ,
         |    current_transition_order_id varchar   ,
         |    outer_order_id              varchar   ,
         |    outer_order_type            int            ,
         |    province_id                 bigint         ,
         |    city_id                     bigint         ,
         |    device_code                 varchar    ,
         |    sales_clerk_id              bigint         ,
         |    dis_member_id               bigint         ,
         |    guide_link_id               varchar   ,
         |    group_buy_type              varchar    ,
         |    refund_type                 varchar    ,
         |    refund_state                varchar    ,
         |    refund_status               varchar    ,
         |    refund_creator              varchar    ,
         |    refund_stock_handle_type    varchar    ,
         |    ref_id                      varchar    ,
         |    refund_at                   TIMESTAMP       ,
         |    erp_order_at                varchar     ,
         |    complete_at                 TIMESTAMP       ,
         |    order_at                    TIMESTAMP       ,
         |    pay_at                      TIMESTAMP       ,
         |    cancel_at                   TIMESTAMP       ,
         |    created_at                  TIMESTAMP       ,
         |    updated_at                  TIMESTAMP       ,
         |    completed_by                varchar  ,
         |    refund_by                   varchar    ,
         |    created_by                  varchar    ,
         |    updated_by                  varchar    ,
         |    last_updated_at             TIMESTAMP   ,
         |    PRIMARY KEY(id) NOT ENFORCED
         |)WITH(
         |    'connector' = 'mysql-cdc',
         |    'hostname' = '10.150.20.11',
         |    'port' = '3306','username' = 'readonly',
         |    'password' = '4Md0IGoBE6',
         |    'database-name' = 'order_db',
         |    'table-name' = 'order_header')
      """.stripMargin

    var kafkaSink:String =
      """CREATE TABLE order_db_order_header_ck (
        |     `id`          					bigint  	    ,
        |    dt               				date 	        ,
        |    `tenant_id`   					varchar  	    ,
        |    `serial_number` 				varchar 			,
        |    `order_category` 				varchar 			,
        |    `order_type` 					varchar 			,
        |    `order_status` 					varchar 			,
        |    `title` 						varchar 			,
        |    `product_count` 				Decimal(19, 2) 	,
        |    `customer_id` 					varchar 			,
        |    `customer_name` 				varchar 			,
        |    `customer_phone` 				varchar 			,
        |    `customer_type` 				bigint 			,
        |    `shop_id` 						varchar 			,
        |    `shop_name` 					varchar 			,
        |    `payment_type` 					varchar 			,
        |    `order_sub_type` 				varchar 			,
        |    `channel_id` 					bigint 			,
        |    `deal_amt` 						Decimal(19, 2) 	,
        |    `paid_amt` 						Decimal(19, 2) 	,
        |    `p_paid_amt` 					Decimal(19, 2) 	,
        |    `f_paid_amt` 					Decimal(19, 2) 	,
        |    `p_pay_amt` 					Decimal(19, 2) 	,
        |    `f_pay_amt` 					Decimal(19, 2) 	,
        |    `p_lp_amt` 						Decimal(19, 2) 	,
        |    `p_sp_amt` 						Decimal(19, 2) 	,
        |    `total_promo_amt` 				Decimal(19, 2) 	,
        |    `p_promo_amt` 					Decimal(19, 2) 	,
        |    `f_amt` 						Decimal(19, 2) 	,
        |    `f_promo_amt` 					Decimal(19, 2) 	,
        |    `p_credit_pay_amt` 				Decimal(19, 2) 	,
        |    `f_credit_pay_amt` 				Decimal(19, 2) 	,
        |    `p_member_store_pay_amt` 		Decimal(19, 2) 	,
        |    `f_member_store_pay_amt` 		Decimal(19, 2) 	,
        |    `p_cash_pay_amt` 				Decimal(19, 2) 	,
        |    `p_change_amt` 					Decimal(19, 2) 	,
        |    `third_party_pay_rate` 			Decimal(9, 4) 	,
        |    `third_party_pay_charge_amt` 	Decimal(9, 2) 	,
        |    `remark` 						varchar 			,
        |    `internal_remark` 				varchar 			,
        |    `reason` 						varchar 			,
        |    `images` 						varchar  		,
        |    `first_buy_flag` 				bigint 			,
        |    `source_order_id` 				bigint 			,
        |    `latest_transition_order_id` 	varchar 			,
        |    `current_transition_order_id` 	varchar 			,
        |    `outer_order_id` 				varchar       	,
        |    `outer_order_type` 				int     	,
        |    `province_id` 					bigint 			,
        |    `city_id` 						bigint 			,
        |    `device_code` 					varchar 			,
        |    `sales_clerk_id` 				bigint 			,
        |    `dis_member_id` 				bigint 			,
        |    `guide_link_id` 				varchar 			,
        |    `group_buy_type` 				varchar 			,
        |    `refund_type` 					varchar 			,
        |    `refund_state` 					varchar 			,
        |    `refund_status` 				varchar 			,
        |    `refund_creator` 				varchar 			,
        |    `refund_stock_handle_type` 		varchar 			,
        |    `ref_id` 						varchar 			,
        |    `refund_at` 					TIMESTAMP   		,
        |    `erp_order_at` 					varchar 			,
        |    `complete_at` 					TIMESTAMP 		,
        |    `order_at` 						TIMESTAMP  		,
        |    `pay_at` 						TIMESTAMP  		,
        |    `cancel_at` 					TIMESTAMP 		,
        |    `refund_by` 					varchar 			,
        |    `completed_by` 					varchar 			,
        |    created_at         				TIMESTAMP        ,
        |    created_by         				varchar          ,
        |    updated_at         				TIMESTAMP        ,
        |    updated_by         				varchar          ,
        |    last_updated_at    				TIMESTAMP        ,
        |    PRIMARY KEY(id,dt) NOT ENFORCED
        |)WITH(
        |'connector'='doris',
        |'fenodes'='10.150.60.2:9030',
        |'username'='root',
        |'password'='bigdata1234',
        |'sink.batch.size'='2',
        |'table.identifier' = 'dev_ods.ods_order_db_order_header_ms'
        |)
      """.stripMargin

    var kafkaSink1:String =
      """CREATE TABLE order_db_order_header_ck (
        |     `id`          					bigint  	    ,
        |    dt               				date 	        ,
        |    PRIMARY KEY(id,dt) NOT ENFORCED
        |)WITH(
        |'connector'='doris',
        |'fenodes'='10.150.60.2:9030',
        |'username'='root',
        |'password'='bigdata1234',
        |'sink.batch.size'='2',
        |'table.identifier' = 'test.ods_order_db_order_header_ms'
        |)
      """.stripMargin
    tableEnv.executeSql(orderSql)
    tableEnv.executeSql(kafkaSink1)
//    tableEnv.executeSql("insert into kafkaSink select i from order_db_order_header_ck")

//    tableEnv.executeSql("insert into order_db_order_header_ck\nselect \nid          \t\t\t\t\n,  dt               \t\t\t\n,  tenant_id\t\t\t\t\n,  serial_number \t\t\t\n,  order_category \t\t\t\n,  order_type \t\t\t\t\n,  order_status \t\t\t\t\n,  title \t\t\t\t\t\n,  product_count \t\t\t\n,  customer_id\n,  customer_name \t\t\t\n,  customer_phone\t\t\t\n,  customer_type \t\t\t\n,  shop_id\t\t\t\t\t\n,  shop_name \t\t\t\t\n,  payment_type \t\t\t\t\n,  order_sub_type \t\t\t\n,  channel_id\t\t\t\t\n,  deal_amt \t\t\t\t\t\n,  paid_amt\t\t\t\t\t\n,  p_paid_amt\t\t\t\t\n,  f_paid_amt \t\t\t\t\n,  p_pay_amt\t\t\t\t\n,  f_pay_amt\t\t\t\t\n,  p_lp_amt\t\t\t\t\t\n,  p_sp_amt\t\t\t\t\t\n,  total_promo_amt\t\t\t\n,  p_promo_amt \t\t\t\t\n,  f_amt\t\t\t\t\t\n,  f_promo_amt\t\t\t\t\n,  p_credit_pay_amt\t\t\t\n,  f_credit_pay_amt\t\t\t\n,  p_member_store_pay_amt \t\n,  f_member_store_pay_amt\t\n,  p_cash_pay_amt\t\t\n,  p_change_amt\t\t\t\n,  third_party_pay_rate\t\t\n,  third_party_pay_charge_amt\n,  remark\t\n,  internal_remark \t\t\t\n,  reason\t\t\t\t\n,  images\t\t\t\t\n,  first_buy_flag\t\t\t\n,  source_order_id\t\t\t\n,  latest_transition_order_id\n,  current_transition_order_id\n,  outer_order_id \t\t\t\n,  outer_order_type \t\t\t\n,  province_id\t\t\t\t\n,  city_id \t\t\t\t\t\n,  device_code \t\t\t\t\n,  sales_clerk_id \t\t\t\n,  dis_member_id \t\t\t\n,  guide_link_id \t\t\t\n,  group_buy_type \t\t\t\n,  refund_type \t\t\t\t\n,  refund_state \t\t\t\t\n,  refund_status \t\t\t\n,  refund_creator \t\t\t\n,  refund_stock_handle_type \t\n,  ref_id \t\t\t\t\t          \n,  refund_at \t\t\t\t\n,  erp_order_at \t\t\t\t\n,  complete_at \t\t\t\t\n,  order_at \t\t\t\t\t\n,  pay_at \t\t\t\t\t\n,  cancel_at\t\t\t\t\n,  refund_by\t\t\t\t\n,  completed_by\t\t\t\t\n,    created_at         \t\t\t\n,    created_by         \t\t\t\n,    updated_at         \t\t\t\n,    updated_by         \t\t\t\n,    last_updated_at    \t\t\t\nfrom  order_db_order_header_ms")
    tableEnv.sqlQuery("select id ,dt from  order_db_order_header_ms").executeInsert("order_db_order_header_ck")


//    tableEnv.toRetractStream[test](table).print()

//    env.execute()


  }

}

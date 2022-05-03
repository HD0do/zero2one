package com.xiaoshouyi

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.MongoRDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document
import com.mongodb.spark._

/**
  * @Author ：zhd
  * @Date: 2021/8/13 11:52
  * @Dscription: 测试是Spark读取MongoDB数据，同时进行做ETL，写入hive
  */
object SparkMongo {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "E:\\software\\hadoop-common-2.2.0")
    //定义配置
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://xsy:boZPJfhORY@10.150.10.7:27017/promotion_db",
      "mongo.db"->"promotion_db"
    )
    //创建spark session
//    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("sparkMongoTest")
//    val spark = SparkSession.builder().config(sparkConf)
//      .config("spark.mongodb.input.uri","mongodb://xsy:boZPJfhORY@10.150.10.7:27017/promotion_db.coupon_ticket")
//      .config("spark.mongodb.input.partitioner","MongoSamplePartitioner")
////     .enableHiveSupport()
//      .getOrCreate()

    var sparkConf: SparkConf = new SparkConf()
                                 .setMaster("local[*]")
                                 .setAppName("nan")
                                 .set("spark.mongodb.input.uri","mongodb://xsy:boZPJfhORY@10.150.10.7:27017/promotion_db.promotion_activity")

    var spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //设置打印日志的级别
    spark.sparkContext.setLogLevel("warn")

    import com.mongodb.spark.config._
//    implicit val mongoConfig = MongoConfig(config("mongo.uri"),config("mongo.db"))

    var ticketDF: DataFrame = MongoSpark.load(spark)
    ticketDF.show()
    ticketDF.printSchema()

   // ticketDF.rdd.map(_.toString()).foreach(print)

    //todo sparksql 选择读取到的MongoDB中一个字段展示
    ticketDF.createOrReplaceTempView("ticket")
    val ticketdf = spark.sql("select productScope.product.include[1] from ticket ")

    ticketdf.show(500)

    spark.close()


//    var promoActRdd = spark
//      .read
//      .option("uri", config("mongo.uri"))
//      .option("collection", "coupon_ticket")
//      .format("com.mongodb.spark.sql")
//      .option("spark.mongodb.input.partitioner", "MongoShardedPartitioner")
//      .option("spark.mongodb.input.partitionerOptions.shardkey","_id")
//      .load()

//    promoActRdd.createOrReplaceTempView("ticket")

//    import spark.implicits._

//    val ticketdf = spark.sql("select couponName from ticket ")
//    ticketdf.show()
//    spark.stop()
//    System.exit(0)

  }
}

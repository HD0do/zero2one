package com.xiaoshouyi.SessionCut

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{RelationalGroupedDataset, Row, SparkSession}

import scala.collection.mutable

/**
  * @Author ：zhd
  * @Date: 2021/12/16 19:04
  * @Dscription:
  */
object SessionCut {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("session").setMaster("local[*]")
    var spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val tablename =
      s"""(select
         |login_id,distinct_id,time from test.test) tmp""".stripMargin

    val readDataDf = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://10.150.60.2:9030")
      .option("fetchsize", "500000")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "bigdata1234")
      .option("dbtable",tablename )
      .load().show()

//    //将Df转换为rdd进行处理
//      val strRDD: RDD[String] = readDataDf.toJSON.rdd
//
//    //数据类型转换
//    val jsonRDD: RDD[JSONObject] = strRDD.map(str => {
//      JSON.parseObject(str)
//    })
//    //根据用户分组
//    val gruopbyUserRDD: RDD[(Any, Iterable[JSONObject])] = jsonRDD.groupBy(json=>json.getString("distinct_id"))
//
//
//    //对组内值排序
//    val sessionRDD = gruopbyUserRDD.mapValues(iter => {
//      //排序
//      val objectsList: List[JSONObject] = iter.toList.sortWith(
//        (left, right) => {
//          left.getIntValue("timestamp") < right.getIntValue("timestamp")
//        })
//
//      //定义切割sessionId
//      //转变为可变数组
//      val jsonArray: mutable.Buffer[JSONObject] = objectsList.toArray.toBuffer
//      var sessionId: String =""
//      sessionId = jsonArray(0).getString("timestamp") + jsonArray(0).getString("distinct_id")
//      for (i <- 0 until jsonArray.length) {
//        if (i > 0) {
//          if (jsonArray(i).getLong("timestamp") - jsonArray(i - 1).getLong("timestamp") > 30*60*1000  ) {
//            sessionId = jsonArray(i).getString("timestamp") + jsonArray(i).getString("distinct_id")
//            println(sessionId)
//            println("***************")
//          }
//        }
//          jsonArray(i).put("distic", sessionId)
//      }
//      jsonArray
//    }
//    )
//
//    sessionRDD.collect().foreach(println)

    spark.stop()

  }
// case class UserLog(login_id:String,distinct_id:String,time:Int)


}


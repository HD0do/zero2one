package com.xiaoshouyi.practice

import com.alibaba.fastjson.JSON
import com.mysql.jdbc.StringUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author ：zhd
  * @Date: 2022/3/9 10:41
  * @Dscription:
  */
object Avg {
  def main(args: Array[String]): Unit = {
    //1.创建spark context实例化对象
    val sparkConf = new SparkConf().setAppName("AVG").setMaster("local[*]")

    //2.sc
    val sc: SparkContext = new SparkContext(sparkConf)

    //3.读取数据
    val tuplesSeq = Seq(
      ("spark", 2), ("hadoop", 6), ("shell", 3), ("hadoop", 4), ("spark", 6)
    )
    val inputRDD = sc.parallelize(tuplesSeq)

    //4.处理数据
    inputRDD.mapValues(line => (line,1))
      .reduceByKey((tmp,item) => (tmp._1+item._1,tmp._2+item._2))
      .map(line => (line._1,line._2._1/line._2._2))
      .foreach(print)

    var json:String = "{\"w\":\"None\"\n}"
     val nObject = JSON.parseObject(json)

    if (nObject.get("w").equals("None")){
      println("zhangh")
    }

    StringUtils.is

    println(nObject.get("w").getClass.getSimpleName)

    sc.stop()

  }

}

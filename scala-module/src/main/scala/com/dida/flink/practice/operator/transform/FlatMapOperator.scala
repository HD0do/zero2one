package com.dida.flink.practice.operator.transform

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


import org.apache.flink.api.scala._
/**
  * @Author ：zhd
  * @Date: 2023/2/28 16:31
  * @Dscription: flatmap 算子
  */
object FlatMapOperator {
  def main(args: Array[String]): Unit = {

    var env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置平行度
    env.setParallelism(1)

    env.fromElements("zhang,zhnag","dfad,fadga","fadg,fadsg,fdsa","dfadgeg,fdsaget")
        .flatMap{value => value.split(",")}
        .print()

    env.execute()

  }

}

package test.base

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object BatchWordCount {
  def main(args: Array[String]): Unit = {

    //创建执行环境
    //1.批式处理
//    val env = ExecutionEnvironment.getExecutionEnvironment
    //2.流式处理
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //source 读取文本文件
    val lineDS = env.readTextFile("flink_scala\\input\\words.txt")

    //转换操作
    //流式操作
    lineDS.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1).print()
    //批处理
    // lineDS.flatMap(_.split(" ")).map((_,1)).groupby(0).sum(1).print()

    env.execute()

  }

}

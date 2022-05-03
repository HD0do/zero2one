package test

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala._//在类型转换的时候没有jar包，需要自己手动导入，保证可以隐式转换
//import org.apache.flink.api.scala._


object FlinkWordCount {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
    // 从文件中读取数据
    val inputPath = "D:\\develop\\java\\flink_java\\input\\words.txt"
    val inputDS: DataStream[String] = env.readTextFile(inputPath)
//    inputDS.print()
//     分词之后， 对单词进行 groupby 分组， 然后用 sum 进行聚合
   val wordCountDS = inputDS.flatMap(_.split("")).map((_, 1)).keyBy(0).sum(1)
//     打印
    wordCountDS.print()

    env.execute()
  }

}

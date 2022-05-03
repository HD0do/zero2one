package test.base

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object NoboundWrodCount {
  def main(args: Array[String]): Unit = {

    //创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //读取数据源source-》socket
    val socketDS = env.socketTextStream("10.150.10.17",9999)
    //转换transform
    socketDS.map((_,1)).keyBy(0).sum(1).print(">>>")

    //z执行环境
    env.execute()
  }

}

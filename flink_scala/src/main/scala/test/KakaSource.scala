package test

import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.api.scala._

object KakaSource {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //kafka连接配置参数
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","10.150.10.15:9092")
    env.setParallelism(1)   //设置并行度为1
    properties.setProperty("group.id","test")
    //读取kafka中数据
    val kafkaDS = env.addSource(new FlinkKafkaConsumer[String]("demo_result",new SimpleStringSchema(),properties))
    //order_db 修改数据条数
    kafkaDS.map(line =>{
      val jsonObject = JSON.parseObject(line)
      jsonObject
    }).filter(data => data.containsKey("data") ).print(
      """
        |>>>
        |###
      """)

    env.execute()
  }
}

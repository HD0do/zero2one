package com.dida.flink.utils

import java.util.Properties

import org.apache.flink.shaded.akka.org.jboss.netty.handler.codec.string.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer


object KafkaUtil {

  //TODO kafka地址
  private val properties: Properties = PropertiesUtil.load("config.properties")
  private val broker_list: String = properties.getProperty("kafka.broker.list")

  collection.mutable.Map(
    "bootstrap.servers" -> broker_list,//初始化连接到集群的地址
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    //定义消费者组
  )


}

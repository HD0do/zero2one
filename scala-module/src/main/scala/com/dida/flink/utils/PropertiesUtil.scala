package com.dida.flink.utils

import java.io.{InputStream, InputStreamReader}
import java.util.Properties

/**
  * 加载配置文件工具类
  */
object PropertiesUtil {

  def load(propertiesName:String)={

    val pro: Properties = new Properties()
    pro.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),"UTF-8"))
    pro
  }

  def main(args: Array[String]): Unit = {
    val pro: Properties = PropertiesUtil.load("config.properties")

    println(pro.getProperty("kafka.broker.list"))
  }

}

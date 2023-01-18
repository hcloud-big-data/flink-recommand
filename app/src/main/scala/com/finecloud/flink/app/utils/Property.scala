package com.finecloud.flink.app.utils

import java.io.{IOException, InputStreamReader}
import java.util.Properties

object Property {

  private val CONF_NAME = "conf.properties"
  private var contextProperties: Properties = _

  { // 初始化
    val in = Thread.currentThread().getContextClassLoader.getResourceAsStream(CONF_NAME)
    contextProperties = new Properties()
    try {
      val reader = new InputStreamReader(in, "UTF-8")
      contextProperties.load(reader)
    } catch {
      case e: IOException =>
        System.err.println(">> >flink conf.properties <<<资源文件加载失败!")
        e.printStackTrace()
    }
  }

  def getProperty(key: String): String = {
    contextProperties.getProperty(key)
  }

  def getProperty(key: String, defaultValue: String): String = {
    contextProperties.getProperty(key, defaultValue)
  }

  def getIntValue(key: String): Int = {
    contextProperties.getProperty(key).toInt
  }

  def getStrValue(key: String): String = contextProperties.getProperty(key)

  def getKafkaProperties(groupId: String): Properties = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", getProperty("kafka.bootstrap.servers"))
    properties.setProperty("zookeeper.connect", getProperty("kafka.zookeeper.connect"))
    properties.setProperty("group.id", groupId)
    properties
  }

}

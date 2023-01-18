package com.finecloud.flink.app.task

import com.finecloud.flink.app.map.LogMapFunction
import com.finecloud.flink.app.utils.Property
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

// 日志任务
object LogTask {

  private val KAFKA_GROUP_ID = "log"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = Property.getKafkaProperties(KAFKA_GROUP_ID)
    val dataSource = env.addSource(new FlinkKafkaConsumer[String]("con", new SimpleStringSchema(), properties))
    dataSource.map(new LogMapFunction())

    env.execute("log message receive")
  }

}

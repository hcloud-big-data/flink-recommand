package com.finecloud.flink.app.task

import com.finecloud.flink.app.map.ProductPortraitMapFunction
import com.finecloud.flink.app.utils.Property
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object ProductPortraitTask {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    val properties = Property.getKafkaProperties("ProductPortrait")
    val source = KafkaSource.builder[String]()
      .setProperties(properties)
      .setTopics("con")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val dataStream = env.fromSource[String](source, WatermarkStrategy.noWatermarks(), "Kafka Source")
    dataStream.map(new ProductPortraitMapFunction())
    env.execute()
  }
}

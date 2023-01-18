package com.finecloud.flink.app.task

import com.finecloud.flink.app.agg.CountAgg
import com.finecloud.flink.app.domain.{LogEntity, TopProductEntity}
import com.finecloud.flink.app.map.TopProductMapFunction
import com.finecloud.flink.app.sink.TopNHotSink
import com.finecloud.flink.app.top.TopNHotItems
import com.finecloud.flink.app.utils.Property
import com.finecloud.flink.app.window.WindowResultFunction
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object TopProductTask {
  private final val topSize = 5

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    env.setParallelism(1)

    val properties = Property.getKafkaProperties("topProduct")
    val source = KafkaSource.builder[String]()
      .setProperties(properties)
      .setTopics("con")
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    // declaration redis connect link

    val dataStream = env.fromSource[String](source, WatermarkStrategy.noWatermarks(), "Kafka Source")
    val topProduct = dataStream.map[LogEntity](new TopProductMapFunction())
      .assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps[LogEntity]().withTimestampAssigner((element, _) => element.time * 1000))
      .keyBy((value: LogEntity) => value.productId)
      .window(TumblingEventTimeWindows.of(Time.seconds(60), Time.seconds(40)))
      .aggregate(new CountAgg(), new WindowResultFunction())
      .keyBy((value: TopProductEntity) => value.windowEnd)
      .process(new TopNHotItems(topSize))
      .flatMap((value, collector: Collector[TopProductEntity]) => {
        println("-----------Top N Product-----------")
        for (elem <- value.indices) {
          val top = new TopProductEntity()
          top.rankName = elem.toString
          top.productId = elem
          println(top)
          collector.collect(top)
        }
      })
    topProduct.addSink(new TopNHotSink)

    env.execute("Top N")
  }
}

package com.finecloud.flink.app.sink

import com.finecloud.flink.app.client.HbaseClient
import com.finecloud.flink.app.domain.TopProductEntity
import org.apache.flink.streaming.api.functions.sink.SinkFunction

class TopNHotSink extends SinkFunction[TopProductEntity] {
  override def invoke(value: TopProductEntity, context: SinkFunction.Context): Unit = {
    val rowKey = value.productId + "_" + value.rankName
    HbaseClient.putData("conn", rowKey, "log", "action", value.productId.toString)
  }
}

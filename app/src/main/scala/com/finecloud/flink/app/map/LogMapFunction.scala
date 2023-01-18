package com.finecloud.flink.app.map

import com.finecloud.flink.app.client.HbaseClient
import com.finecloud.flink.app.domain.LogEntity
import com.finecloud.flink.app.utils.LogToEntity
import org.apache.flink.api.common.functions.MapFunction

class LogMapFunction extends MapFunction[String, LogEntity] {
  override def map(value: String): LogEntity = {
    val log = LogToEntity.getLog(value)
    if (log != null) {
      val rowKey = log.userId + "_" + log.productId + "_" + log.time
      HbaseClient.putData("conn", rowKey, "log", "userid", log.userId.toString)
      HbaseClient.putData("conn", rowKey, "log", "productid", log.productId.toString)
      HbaseClient.putData("conn", rowKey, "log", "time", log.time.toString)
      HbaseClient.putData("conn", rowKey, "log", "action", log.action)
    }
    log
  }
}


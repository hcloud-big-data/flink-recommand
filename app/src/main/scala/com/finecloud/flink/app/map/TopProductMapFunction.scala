package com.finecloud.flink.app.map

import com.finecloud.flink.app.domain.LogEntity
import com.finecloud.flink.app.utils.LogToEntity
import org.apache.flink.api.common.functions.RichMapFunction

class TopProductMapFunction extends RichMapFunction[String, LogEntity]{
  override def map(value: String): LogEntity = {
    LogToEntity.getLog(value)
  }
}

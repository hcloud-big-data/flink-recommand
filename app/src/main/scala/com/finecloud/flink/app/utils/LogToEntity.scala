package com.finecloud.flink.app.utils

import com.finecloud.flink.app.domain.LogEntity

object LogToEntity {

  def getLog(log: String): LogEntity = {

    val value = log.split(",")
    if (value.length < 2) {
      System.out.println("log format error")
      return null
    }
    val logEntity = new LogEntity
    logEntity.userId = value(0).toInt
    logEntity.productId = value(1).toInt
    logEntity.time = value(2).toInt
    logEntity.action = value(3)
    logEntity
  }

}

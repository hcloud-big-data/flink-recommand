package com.finecloud.flink.app.domain

case class TopProductEntity(var productId: Int = _, var actionTimes: Int = _, var windowEnd: Long = _, var rankName: String = _)

object TopProductEntity {
  def of(itemId: Int, end: Long, count: Long): TopProductEntity = new TopProductEntity(itemId, count.intValue(), end, end.toString)
}
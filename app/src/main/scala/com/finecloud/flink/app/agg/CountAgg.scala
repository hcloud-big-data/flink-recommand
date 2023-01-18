package com.finecloud.flink.app.agg

import com.finecloud.flink.app.domain.LogEntity
import org.apache.flink.api.common.functions.AggregateFunction

class CountAgg extends AggregateFunction[LogEntity, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: LogEntity, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

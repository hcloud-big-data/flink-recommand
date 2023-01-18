package com.finecloud.flink.app.window

import com.finecloud.flink.app.domain.TopProductEntity
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.lang

class WindowResultFunction extends WindowFunction[Long, TopProductEntity, Int, TimeWindow] {
  override def apply(key: Int, window: TimeWindow, input: lang.Iterable[Long], out: Collector[TopProductEntity]): Unit = {
    val itemId = key
    val count = input.iterator.next()
    out.collect(TopProductEntity.of(itemId, window.getEnd, count))
  }
}

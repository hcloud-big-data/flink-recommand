package com.finecloud.flink.app.top

import com.finecloud.flink.app.domain.TopProductEntity
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

class TopNHotItems(val topSize: Int) extends KeyedProcessFunction[Long, TopProductEntity, List[String]] {
  private var itemState: ListState[TopProductEntity] = null

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, TopProductEntity, List[String]]#OnTimerContext, out: Collector[List[String]]): Unit = {
    val allItems: List[TopProductEntity] = List()
    itemState.get().forEach(item => item +: allItems)
    itemState.clear()
    allItems.sortWith((first, second) => (first.actionTimes - second.actionTimes) > 0)
    val ret: List[String] = List()
    allItems.foreach(item => ret +: item.productId.toString)
    out.collect(ret)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val itemsStateDesc = new ListStateDescriptor("itemState-state", classOf[TopProductEntity])
    itemState = getRuntimeContext.getListState(itemsStateDesc)
  }

  override def processElement(value: TopProductEntity, ctx: KeyedProcessFunction[Long, TopProductEntity, List[String]]#Context, out: Collector[List[String]]): Unit = {
    itemState.add(value)
    //  Registering the EventTime Timer for windowEnd plus 1, when triggered, indicates that all the items belonging to the windowEnd window have been collected
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }
}

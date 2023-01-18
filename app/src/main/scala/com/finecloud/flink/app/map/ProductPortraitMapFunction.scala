package com.finecloud.flink.app.map

import com.finecloud.flink.app.client.{HbaseClient, MysqlClient}
import com.finecloud.flink.app.utils.{AgeUtil, LogToEntity}
import org.apache.flink.api.common.functions.RichMapFunction

class ProductPortraitMapFunction extends RichMapFunction[String, String] {
  override def map(value: String): String = {
    val log = LogToEntity.getLog(value)
    val rst = MysqlClient.selectById(log.userId)
    if (rst != null) {
      while (rst.next) {
        val productId = log.productId.toString
        val sex = rst.getString("sex")
        HbaseClient.increamColumn("prod", productId, "sex", sex)
        val age = rst.getString("age")
        HbaseClient.increamColumn("prod", productId, "age", AgeUtil.getAgeType(age))
      }
    }
    null
  }
}

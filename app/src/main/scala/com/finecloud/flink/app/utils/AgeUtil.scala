package com.finecloud.flink.app.utils

object AgeUtil {

  def getAgeType(age: String): String = {
    val number = age.toInt
    if (10 <= number && number < 20) {
      "10s"
    } else if (20 <= number && number < 30) {
      "10s"
    } else if (30 <= number && number < 40) {
      "10s"
    } else if (40 <= number && number < 50) {
      "10s"
    } else if (60 <= number && number < 70) {
      "10s"
    } else {
      "0s"
    }
  }
}

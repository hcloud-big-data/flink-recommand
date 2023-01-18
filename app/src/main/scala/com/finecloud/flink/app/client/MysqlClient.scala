package com.finecloud.flink.app.client

import com.finecloud.flink.app.utils.Property

import java.sql.{DriverManager, ResultSet, SQLException, Statement}

object MysqlClient {
  private val URL = Property.getStrValue("mysql.url")
  private val NAME = Property.getStrValue("mysql.name")
  private val PASS = Property.getStrValue("mysql.pass")
  private val stmt: Statement = {
    var statement: Statement = null
    try {
      Class.forName("com.mysql.cj.jdbc.Driver")
      val conn = DriverManager.getConnection(URL, NAME, PASS)
      statement = conn.createStatement()
    } catch {
      case e: ClassNotFoundException => e.printStackTrace()
      case e: SQLException => e.printStackTrace()
    }
    statement
  }

  @throws[SQLException]
  def selectById(id: Int): ResultSet = {
    val sql = String.format("select  * from product where product_id = %s", id)
    stmt.executeQuery(sql)
  }

  @throws[SQLException]
  def selectUserById(id: Int): ResultSet = {
    val sql = String.format("select  * from user where user_id = %s", id)
    stmt.executeQuery(sql)
  }

}

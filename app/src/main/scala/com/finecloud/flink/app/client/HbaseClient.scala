package com.finecloud.flink.app.client

import com.finecloud.flink.app.utils.Property
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import java.io.IOException
import scala.collection.JavaConverters.asScalaIteratorConverter

object HbaseClient {
  private var admin: Admin = _
  private var conn: Connection = _

  {
    val conf = HBaseConfiguration.create
    conf.set("hbase.rootdir", Property.getProperty("hbase.rootdir"))
    conf.set("hbase.zookeeper.quorum", Property.getProperty("hbase.zookeeper.quorum"))
    conf.set("hbase.client.scanner.timeout.period", Property.getProperty("hbase.client.scanner.timeout.period"))
    conf.set("hbase.rpc.timeout", Property.getProperty("hbase.rpc.timeout"))
    try {
      conn = ConnectionFactory.createConnection(conf)
      admin = conn.getAdmin
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }

  def createTable(tableName: String, columnFamily: Array[String]) = {
    val tablename = TableName.valueOf(tableName);
    if (admin.tableExists(tablename)) {
      System.out.println("Table Exists")
    } else {
      System.out.println("Start create table")
      val tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tablename)

      columnFamily.foreach(cf => tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(cf.getBytes).build()))
      admin.createTable(tableDescriptorBuilder.build())
      System.out.println("Create Table success");
    }
  }

  def getData(tableName: String, rowKey: String, columnFamily: String, column: String): String = {
    val table = conn.getTable(TableName.valueOf(tableName))
    val get = new Get(rowKey.getBytes)
    get.addColumn(columnFamily.getBytes, column.getBytes)
    val result = table.get(get)
    val value = result.getValue(columnFamily.getBytes, column.getBytes)
    new String(value)
  }

  def getRow(tableName: String, rowKey: String): List[Map[String, String]] = {
    val table = conn.getTable(TableName.valueOf(tableName))
    val get = new Get(rowKey.getBytes)
    val result = table.get(get)
    result.rawCells().zipWithIndex.map {
      case (cell, _) =>
        val columnFamily = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
        val column = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
        (columnFamily, column)
    }.toList.map(x => Map.apply(x._1 -> x._2))
  }

  def putData(tableName: String, rowKey: String, columnFamily: String, column: String, value: String): Unit = {
    val table = conn.getTable(TableName.valueOf(tableName))
    val put = new Put(rowKey.getBytes)
    put.addColumn(columnFamily.getBytes, column.getBytes, value.getBytes)
    table.put(put)
  }

  def increamColumn(tableName: String, rowKey: String, columnFamily: String, column: String): Unit = {
    val dataStr = getData(tableName, rowKey, columnFamily, column)
    var res = 1
    if (dataStr != null) {
      res = dataStr.toInt + 1
    }
    putData(tableName, rowKey, columnFamily, column, res.toString)
  }

  def getAllKeys(tableName: String): List[String] = {
    val table = conn.getTable(TableName.valueOf(tableName))
    val scanner = table.getScanner(new Scan())
    scanner.iterator().asScala.map(x => Bytes.toString(x.getRow)).toList
  }

}

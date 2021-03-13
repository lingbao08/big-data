package com.lingbao.dao

import com.lingbao.commons.Constant
import com.lingbao.config.HbaseFactory
import com.lingbao.entity.GoingCount
import org.apache.hadoop.hbase.client.{Delete, Get, Scan}
import org.apache.hadoop.hbase.filter.PrefixFilter
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object GoingCountDAO {

  /**
    * 保存数据到HBASE
    *
    * @param list
    */
  def save(list: mutable.Buffer[GoingCount]) = {
    val table = HbaseFactory.getInstance().getHTable(Constant.tableName)

    for (elem <- list) {
      table.incrementColumnValue(Bytes.toBytes(elem.getDay_desc), Bytes.toBytes(Constant.cf)
        , Bytes.toBytes(Constant.qualifer), elem.getGo_count)
    }
  }

  /**
    * 根据rowKey查询值
    *
    * @param List
    */
  def count(day_desc: String): Long = {
    val table = HbaseFactory.getInstance().getHTable(Constant.tableName)

    val get = new Get(Bytes.toBytes(day_desc))

    val bytes = table.get(get).getValue(Bytes.toBytes(Constant.cf), Bytes.toBytes(Constant.qualifer))

    if (null == bytes)
      0
    else
      Bytes.toLong(bytes)
  }


  /**
    * 根据rowKey前缀查询值
    *
    * @param List
    */
  def preCount(day: String): ListBuffer[GoingCount] = {
    val table = HbaseFactory.getInstance().getHTable(Constant.tableName)

    val scan = new Scan()
    scan.setFilter(new PrefixFilter(day.getBytes()))

    val scannerResult = table.getScanner(scan)

    val list = new ListBuffer[GoingCount]

    scannerResult.forEach(result => {
      val row = new String(result.getRow)
      val value = Bytes.toLong(result.getValue(Constant.cf.getBytes(), Constant.qualifer.getBytes()))
      list.append(new GoingCount(row, value))
    })
    list
  }

  /**根据rowKey删除整行数据
    *
    * @param day_desc rowKey的值
    */
  def deleteRowKey(day_desc: String) = {

    val table = HbaseFactory.getInstance().getHTable(Constant.tableName)
    val delete = new Delete(day_desc.getBytes())
    table.delete(delete)
  }


}

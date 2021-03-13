package com.lingbao.dao

import com.lingbao.config.JedisUtil
import com.lingbao.entity.GoingCount

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object GoingCountRedisDao {

  /**
    * 保存数据到HBASE
    *
    * @param list
    */
  def save(list: mutable.Buffer[GoingCount]) = {
    for (elem <- list) {
      val sarrs = elem.getDay_desc.split("_")
      JedisUtil.getJedis.hincrBy(sarrs(0), sarrs(1), elem.getGo_count)
    }
  }

  /**
    * 查询某天某地的计数
    *
    * @param List
    */
  def count(day_desc: String): Long = {
    val sarrs = day_desc.split("_")
    JedisUtil.getJedis.hget(sarrs(0), sarrs(1)).toLong
  }


  /**
    * 查询某天所有计数
    *
    * @param List
    */
  def preCount(day: String): ListBuffer[GoingCount] = {

    val allResultMap = JedisUtil.getJedis.hgetAll(day)
    val list = new ListBuffer[GoingCount]
    allResultMap.forEach((x, y) => list.append(new GoingCount(x, y.toLong)))
    list
  }

  /** 根据rowKey删除整行数据
    *
    * @param day_desc rowKey的值
    */
  def deleteRowKey(day_desc: String) = {
    val sarrs = day_desc.split("_")
    JedisUtil.getJedis.hdel(sarrs(0), sarrs(1))
  }

  def main(args: Array[String]): Unit = {
//    JedisUtil.getJedis.del("2019-10-24")

    println(preCount("2019-10-24"))
  }
}

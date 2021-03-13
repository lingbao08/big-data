package com.lingbao.flink.dao

import java.sql.{Connection, PreparedStatement}
import java.text.SimpleDateFormat

import com.lingbao.flink.config.MySQLConfig
import com.lingbao.flink.module.CriminalScore
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

class MyCriminalScoreSQLSource extends RichParallelSourceFunction[CriminalScore] {

  var connection: Connection = _
  var ps: PreparedStatement = _

  //用来建立连接
  override def open(parameters: Configuration): Unit = {

    connection = MySQLConfig.getConn()
    ps = connection.prepareStatement("select id_no,real_name,orignal_score,create_time from criminal_score limit 10")
  }

  override def run(sourceContext: SourceFunction.SourceContext[CriminalScore]): Unit = {

    val queryResult = ps.executeQuery()
    while (queryResult.next) {
      val str = queryResult.getString(1)
      val str2 = queryResult.getString(2)
      val d = queryResult.getDouble(3)
      val dateStr = queryResult.getString(4)
      val date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(dateStr)
      sourceContext.collect(CriminalScore(str, str2, d, date))
    }
  }

  override def close(): Unit = {
    if (ps != null) {
      ps.close()
    }
    if (connection != null) {
      connection.close()
    }
  }

  override def cancel(): Unit = {

  }
}

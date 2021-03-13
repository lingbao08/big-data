package com.lingbao.flink.dao

import java.sql.{Connection, PreparedStatement}

import com.lingbao.flink.config.MySQLConfig
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

class MyRouterConfigSQLSource extends RichParallelSourceFunction[Long] {

  var connection: Connection = _
  var ps: PreparedStatement = _

  var isRunning = true

  //用来建立连接
  override def open(parameters: Configuration): Unit = {

    connection = MySQLConfig.getConn()
    ps = connection.prepareStatement("SELECT id FROM aq_router_config t  limit 3")
  }

  override def run(sourceContext: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning) {
      val queryResult = ps.executeQuery()
      while (queryResult.next) {
        val str = queryResult.getLong(1)
        sourceContext.collect(str)
      }

      Thread.sleep(2000)
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
    isRunning = false
  }
}

package com.lingbao.flink.dao

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object MySQLTest {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 设置只有一个线程去读取数据，否则会出现多行相同的数据
//    val data = env.addSource(new MyCriminalScoreSQLSource).setParallelism(1)
    val data = env.addSource(new MyRouterConfigSQLSource).setParallelism(1)

    data.print()

    env.execute("MySQLTest")

  }

}

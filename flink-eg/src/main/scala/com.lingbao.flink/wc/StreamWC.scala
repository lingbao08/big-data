package com.lingbao.flink.wc

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 测试方法：在dos窗口使用`nc -l 9999`打开连接，输入字符串，然后这边启动就会进行统计信息
  *
  * 下面的示例的窗口期是5秒
  */
object StreamWC {


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("localhost", 9999)

    //每行文字以空格分隔
    text.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      //keyBy(_._1) 和 keyBy(0) 是一个意思，都是这个tuple的第一个值
      .keyBy(_._1)
      // 窗口期是5秒，即每隔5秒统计一次信息
      .timeWindow(Time.seconds(5))
      .sum(1).print()
      .setParallelism(1)

    env.execute("StreamWC  start")

  }


  case class WC(word: String, num: Int)

}

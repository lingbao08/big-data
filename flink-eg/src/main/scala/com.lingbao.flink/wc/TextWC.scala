package com.lingbao.flink.wc

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/**
  * 读取指定文件的文字，并进行统计
  */
object TextWC {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val textFile = env.readTextFile("~/JAVA/base/demo2/flink-demo/test.txt")


    val counts = textFile.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    counts.print()
  }

}

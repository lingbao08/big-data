package com.lingbao.spark

import java.time.LocalDate

import com.lingbao.commons.Constant
import com.lingbao.dao.GoingCountRedisDao
import com.lingbao.entity.{Going, GoingCount}
import com.lingbao.util.DateUtil
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object LoggerStreaming {
  def main(args: Array[String]): Unit = {


    val Array(brokers, groupId, topics) = Array(Constant.KAFKA_BROKER_LIST,
      Constant.KAFKA_GROUP_ID, Constant.KAFKA_TOPIC)


    val sparkConf = new SparkConf().setAppName("LoggerStreaming").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(30))

    //    ssc.checkpoint(Constant.KAFKA_CHECK_POINT_DIR)

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      //这个config默认是true，他会周期性想kafka提交offset信息
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      //默认是laster，即从队尾进行消费。
      // 需要设置为从队首进行消费，然后跳转到指定的offset位置
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    //数据示例：
    //2019-10-17       0
    // 17:28:01        1
    // 8.98.91.17        2
    // GET              3
    // going/112.html   4
    // HTTP1.1           5
    // https://search.yahoo.com/search?p=北京     6
    // 505               7
    //这个map是获取到所有的数据（你的一行数据在这里就对应一行）
    messages.foreachRDD { rdd =>
      val lines = rdd.map(_.value())

      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      //第一个map是将每一行数据按照空格分开，但是分开后的长度和lines是一样的，只是内部变成了Tuple
      //Tuple可以理解为Scala自带的一个可以有多个属性的对象
      //也就是说，lines.map后的Size和lines是一样的，lines的子元素是字串，map后的子元素是tuple
      val lm = lines.map(_.split(" ")).map(l => {
        new Going(DateUtil.getDate(l(0) + " " + l(1)), l(2), l(4), l(6))
      }).filter(g => g.getUrl.startsWith("going"))


      //测试的第二步：写入HBASE
      //过滤掉-的区域
      lm.filter(going => going.getHttpRef != "-")
        //拼凑成tuple，然后reduceByKey，统计每个地方的次数
        .map(going => (LocalDate.from(going.getDate) + "_"
        + going.getHttpRef.substring(going.getHttpRef.lastIndexOf("=") + 1), 1)).reduceByKey(_ + _)
        //以foreachPartition的形式写入到数据库中，使用分区的形式写入到HBASE中
        .foreachPartition(partition => {
        //这句print可以打印出各种相关信息   topic，分区编号，开始偏移量，结尾偏移量
        //        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        //println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        val list = new ListBuffer[GoingCount]
        partition.foreach(pair => {
          list.append(new GoingCount(pair._1, pair._2))
        })
        GoingCountRedisDao.save(list)

      })
      //线程安全的方式进行提交
      messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }


    ssc.start()
    ssc.awaitTermination()
  }

}
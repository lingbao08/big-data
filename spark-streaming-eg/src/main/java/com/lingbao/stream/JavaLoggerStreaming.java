package com.lingbao.stream;

import com.lingbao.commons.Constant;
import com.lingbao.dao.GoingCountRedisDao;
import com.lingbao.entity.Going;
import com.lingbao.entity.GoingCount;
import com.lingbao.util.DateUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;
import scala.collection.JavaConverters;

import java.time.LocalDate;
import java.util.*;

/**
 * @author lingbao08
 * @DESCRIPTION
 * @create 2019-10-18 23:00
 **/

public final class JavaLoggerStreaming {

    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("JavaLoggerStreaming");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(30));

//        jssc.checkpoint(Constant.KAFKA_CHECK_POINT_DIR);

        Set<String> topicsSet = new HashSet<>(Arrays.asList(Constant.KAFKA_TOPIC.split(",")));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.KAFKA_BROKER_LIST);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, Constant.KAFKA_GROUP_ID);
        //周期性提交offset
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        //默认是laster，即从队尾进行消费。
        // 需要设置为从队首进行消费，然后跳转到指定的offset位置
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);


        JavaInputDStream<ConsumerRecord<String, String>> messages =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topicsSet, kafkaParams)
                );

        messages.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

            JavaRDD<String> lines = rdd.map(ConsumerRecord::value);
            JavaRDD<Going> lm = lines.map(l -> l.split(" ")).map(l ->
                    new Going(DateUtil.getDate(l[0] + " " + l[1]), l[2], l[4], l[6])
            ).filter(g -> g.getUrl().startsWith("going"));


            //注意此处，Scala中只有=，java中药区分equals和=
            lm.filter(going -> !going.getHttpRef().equals("-"))
                    .mapToPair(going -> (new Tuple2<>(LocalDate.from(going.getDate()) + "_"
                            + going.getHttpRef().substring(going.getHttpRef().lastIndexOf("=") + 1), 1)))
                    .reduceByKey((i1, i2) -> i1 + i2)
                    .foreachPartition(consumerRecords -> {
                        List<GoingCount> list = new ArrayList<>();
                        consumerRecords.forEachRemaining(pair -> {
                            list.add(new GoingCount(pair._1, pair._2));
                        });
//                        OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
//                System.out.println(
//                        o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
                        GoingCountRedisDao.save(JavaConverters.asScalaBuffer(list));

                    });
            ((CanCommitOffsets) messages.inputDStream()).commitAsync(offsetRanges);
        });

        jssc.start();
        jssc.awaitTermination();
    }
}

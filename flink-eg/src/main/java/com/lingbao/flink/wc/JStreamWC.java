package com.lingbao.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.stream.Stream;

/**
 * @author lingbao08
 * @DESCRIPTION java实时流处理词频统计
 * @create 2019-11-02 16:55
 **/

public class JStreamWC {

    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // get input data
        DataStream<String> text = env.socketTextStream("localhost", 9999);


        DataStream<Tuple2<String, Integer>> counts =
                text.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) ->
                        Stream.of(s.split(" ")).filter(v -> v.length() > 0)
                                .forEach(
                                        v -> collector.collect(new Tuple2<>(v, 1))
                                )
                ).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy(0)
                        .timeWindow(Time.seconds(5))
                        .sum(1);

        counts.print().setParallelism(1);

        // execute program
        env.execute("Streaming WordCount");
    }

}

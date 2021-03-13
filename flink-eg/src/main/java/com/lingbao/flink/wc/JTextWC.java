package com.lingbao.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.stream.Stream;

/**
 * @author lingbao08
 * @DESCRIPTION
 * @create 2019-09-15 13:23
 **/

public class JTextWC {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


        DataSource<String> textFile = env.readTextFile("~/JAVA/base/demo2/flink-demo/test.txt");

//        DataSet<Tuple2<String, Integer>> counts =
//                textFile.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//
//                    @Override
//                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
//                        String[] split = s.split("");
//                        for (String s1 : split) {
//                            if (s1.length() > 0)
//                                collector.collect(new Tuple2<String, Integer>(s1, 1));
//                        }
//                    }
//                }).groupBy(0)
//                        .sum(1);


        DataSet<Tuple2<String, Integer>> counts =
                textFile.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) ->
                        Stream.of(s.split(" "))
                                .forEach(
                                        v -> collector.collect(new Tuple2<>(v, 1))
                                )
                ).returns(Types.TUPLE(Types.STRING, Types.INT)).groupBy(0)
                        .sum(1);


        counts.print();


    }
}

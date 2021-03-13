package com.lingbao.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author lingbao08
 * @DESCRIPTION  指定文件夹分析词频
 * @create 2019-09-28 22:31
 **/

public class UrlWCTopology {

    public static final String url = "/Users/lingbao08/Documents/storm/";

    public static class DataSourceSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;

        //初始化方法
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        /**
         * 接收或者生产数据的地方
         * <p>
         * nextTuple 这个方法是一个死循环，所以读文件会一直读
         */
        @Override
        public void nextTuple() {

            try {
                //读取文件
                Files.list(Paths.get(url)).forEach(path -> {

                    Utils.sleep(1000);
                    try {

                        boolean b = path.toString().endsWith("txt");
//                        System.out.println(b+"-----"+path.toString());
                        if (!b) {
                            return;
                        }
                        System.out.println("ZZZZZZZZZZZZ");
                        List<String> words = Files.readAllLines(path, Charset.defaultCharset());
                        Files.move(path, Paths.get(path.getParent() + File.separator + path.getFileName() + ".bak"));
                        words.forEach(
                                word -> {
                                    collector.emit(new Values(word));
                                    System.out.println("第一步：" + word);
                                }
                        );


                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });


            } catch (IOException e) {
                e.printStackTrace();
            }

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("Split"));
        }
    }

    public static class SplitBolt extends BaseRichBolt {

        private OutputCollector collector;


        //不需要再传出去了，所以就不再定义collector了
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }


        @Override
        public void execute(Tuple input) {

            String cc = input.getStringByField("Split");

            Stream.of(cc.split(",")).forEach(c -> collector.emit(new Values(c)));

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("Count"));
        }
    }

    public static class CountBolt extends BaseRichBolt {

        //不需要再传出去了，所以就不再定义collector了
        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        }

        HashMap<String, Integer> map = new HashMap();

        @Override
        public void execute(Tuple input) {
            String cc = input.getStringByField("Count");

            Integer count = map.get(cc);

            count = count == null ? 0 : count;
            count++;

            map.put(cc, count);

            System.out.println("-------------------------");
            map.entrySet().forEach(e -> {
                System.out.println("输出的内容：" + e);
            });
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) {
        LocalCluster localCluster = new LocalCluster();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("SP", new UrlWCTopology.DataSourceSpout());
        builder.setBolt("SB0", new UrlWCTopology.SplitBolt()).shuffleGrouping("SP");
        builder.setBolt("SB1", new UrlWCTopology.CountBolt()).shuffleGrouping("SB0");

        //参数分别是：定义topology的名字，配置，topology
        localCluster.submitTopology("UrlWCTopology", new Config(), builder.createTopology());


    }
}

package com.lingbao.storm;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * @author lingbao08
 * @DESCRIPTION 基本类型测试
 * @create 2019-09-28 21:40
 **/

public class BaseSumTopology {


    public static class DataSourceSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;
        private int num;

        //初始化方法
        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        /**
         * 接收或者生产数据的地方
         */
        @Override
        public void nextTuple() {
            collector.emit(new Values(++num));
            System.out.println("spout " + num);
            Utils.sleep(1000);

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("ZZ"));
        }


        @Override
        public void ack(Object msgId) {
            System.out.println("msgId:" + msgId + "发送成功了");
            super.ack(msgId);
        }

        @Override
        public void fail(Object msgId) {
            //失败重新发送
            collector.emit(new Values(++num));
        }
    }

    public static class SumBolt extends BaseBasicBolt {

        //不需要再传出去了，所以就不再定义collector了

        int sum = 0;

        @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            Integer zz = input.getIntegerByField("ZZ");
            System.out.println("当前线程ID：" + Thread.currentThread().getId() + "获取到的值：" + zz);
            System.out.println("bolt 求和 " + (sum += zz));

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }

    public static void main(String[] args) throws Exception {

        //定义topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("SP", new DataSourceSpout(), 1);
        builder.setBolt("SB", new SumBolt(), 3)
                .shuffleGrouping("SP");

        //定义config
        Config config = new Config();

        //参数分别是：定义topology的名字，配置，topology
        StormSubmitter.submitTopology("ClusterSumTopology", config, builder.createTopology());

    }


}

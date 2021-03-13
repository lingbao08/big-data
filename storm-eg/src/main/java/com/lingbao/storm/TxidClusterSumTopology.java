package com.lingbao.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;

/**
 * @author lingbao08
 * @DESCRIPTION 事务型测试
 * @create 2019-09-28 21:40
 **/

public class TxidClusterSumTopology {


    public static class DataSourceSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;

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
            collector.emit(new Values(1, 2, 3));
            Utils.sleep(1000);
            collector.emit(new Values(4, 1, 6));
            Utils.sleep(1000);
            collector.emit(new Values(3, 0, 8));
            Utils.sleep(300000000);

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("a", "b", "c"));
        }


        @Override
        public void ack(Object msgId) {
            System.out.println("msgId:" + msgId + "发送成功了");
            super.ack(msgId);
        }

        @Override
        public void fail(Object msgId) {
            System.out.println("发送失败了");
            //失败重新发送
//            collector.emit(new Values(++num));
        }
    }


    public static class MyFunction extends BaseFunction {
        public void execute(TridentTuple tuple, TridentCollector collector) {
            for (int i = 0; i < tuple.getInteger(0); i++) {
                collector.emit(new Values(i));
            }
        }
    }


    public static void main(String[] args) throws Exception {

        //定义config
        Config config = new Config();


        TridentTopology topology = new TridentTopology();
        topology.newStream("spout1", new DataSourceSpout())
                .each(new Fields("a"), new MyFunction(), new Fields("d"))
                .map(e -> {
                    System.out.print(e.getInteger(0));
                    System.out.print(e.getInteger(1));
                    System.out.print(e.getInteger(2));
                    System.out.println(e.getInteger(3));
                    return null;
                });

        LocalCluster cluster = new LocalCluster();
//参数分别是：定义topology的名字，配置，topology
        cluster.submitTopology("ClusterSumTopology", config, topology.build());


    }


}

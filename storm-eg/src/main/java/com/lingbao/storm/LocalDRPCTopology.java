package com.lingbao.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * @author lingbao08
 * @DESCRIPTION 本地远程调用测试
 * @create 2019-10-01 19:53
 **/

public class LocalDRPCTopology {


    public static class MyBolt extends BaseRichBolt {

        private OutputCollector collector;

        @Override
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        @Override
        public void execute(Tuple input) {
            Object id = input.getValue(0);
            String string = input.getString(1);

            //业务逻辑
            String result = "我是" + string + "，听到请回答！";

            collector.emit(new Values(id, result));
        }

        public void execute1(Tuple input) {
            Object id = input.getValue(0);
            String string = input.getString(1);

            //业务逻辑
            String result = "我是" + string + "，听到请回答！";

            collector.emit(input,new Values(id,result));
            collector.ack(input);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "result"));
        }
    }


    public static void main(String[] args) {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("add user");
        builder.addBolt(new MyBolt());

        LocalCluster cluster = new LocalCluster();
        LocalDRPC drpc = new LocalDRPC();

        Config config = new Config();
        config.setNumAckers(0);

        HashMap map = new HashMap();
        map.put(Config.TOPOLOGY_ACKER_EXECUTORS,0);

        cluster.submitTopology("local-drpc", new Config(), builder.createLocalTopology(drpc));

        String execute = drpc.execute("add user", "李四");

        System.out.println("结果：" + execute);

        cluster.shutdown();
        drpc.shutdown();
    }


}

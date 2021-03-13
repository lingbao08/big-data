package com.lingbao.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;


/**
 * @author lingbao08
 * @DESCRIPTION
 * @create 2019-10-21 15:26
 **/

public class HbaseFactory {

    HBaseAdmin admin = null;
    Configuration configuration;


    private HbaseFactory() {

        configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", "192.168.199.119:2181");
        configuration.set("hbase.rootdir", "hdfs://192.168.199.119:8020/hbase");

        try {
            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static HbaseFactory instance = null;

    public static synchronized HbaseFactory getInstance() {
        if (instance == null) {
            instance = new HbaseFactory();
        }
        return instance;
    }


    public HTable getHTable(String tableName) {

        HTable hTable = null;
        try {
            hTable = new HTable(configuration, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return hTable;
    }








}

package com.lingbao.sparkshow.dao;

import com.lingbao.config.HbaseFactory;
import com.lingbao.entity.GoingCount;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.lingbao.commons.Constant.*;

/**
 * @author lingbao08
 * @DESCRIPTION
 * @create 2019-10-23 17:47
 **/
@Component
public class GoingCountDao {


    @Autowired
    private HbaseFactory hbaseConn;

    /**
     * 根据rowKey前缀查询值
     *
     * @param day
     */
    public List<GoingCount> preRowKeyCount(String day) throws IOException {
        HTable table = hbaseConn.getHTable(tableName);

        Scan scan = new Scan();
        scan.setFilter(new PrefixFilter(day.getBytes()));

        ResultScanner scannerResult = table.getScanner(scan);

        List list = new ArrayList<>();

        scannerResult.forEach(result -> {
            String row = new String(result.getRow());
            Long value = Bytes.toLong(result.getValue(cf.getBytes(), qualifer.getBytes()));
            list.add(new GoingCount(row, value));
        });
        return list;
    }

}

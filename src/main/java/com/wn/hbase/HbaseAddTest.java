package com.wn.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseAddTest {
    public static void main(String[] args) throws Exception {

        Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum", "server3:2181");
        cfg.set("hbase.zookeeper.property.clientPort", "2181");
        cfg.set("hbase.defaults.for.version.skip", "true");

        Connection connection = ConnectionFactory.createConnection(cfg);

        Table table = connection.getTable(TableName.valueOf("wn_test"));
        while (true) {
            Put put = new Put(Bytes.toBytes("" + System.currentTimeMillis()));

            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("info"),
                    Bytes.toBytes("abcdefghijklmnopqrstuvwxyz" + System.currentTimeMillis()));
            put.addColumn(Bytes.toBytes("base"), Bytes.toBytes("name"),
                    Bytes.toBytes("yoho" + System.currentTimeMillis()));
            put.addColumn(Bytes.toBytes("base"), Bytes.toBytes("address"),
                    Bytes.toBytes("nanjing" + System.currentTimeMillis()));
            table.put(put);
        }

    }

}

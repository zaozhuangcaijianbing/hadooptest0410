package com.wn.hbase;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HbaseAddTest {
    public static void main(String[] args) throws Exception {

        Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum", "server3:2181");
        cfg.set("hbase.zookeeper.property.clientPort", "2181");
        cfg.set("hbase.defaults.for.version.skip", "true");

        Connection connection = ConnectionFactory.createConnection(cfg);

        Table table = connection.getTable(TableName.valueOf("wn_test"));

        ExecutorService executorService = Executors.newFixedThreadPool(10);

        for (int i = 0; i < 10; i++) {
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    List<Put> list = Lists.newArrayList();

                    while (true) {
                        try {

                            Put put = new Put(Bytes.toBytes(Thread.currentThread().getName() + ":" + System.currentTimeMillis()));

                            put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("info"),
                                    Bytes.toBytes("abcdefghijklmnopqrstuvwxyzasdfasdfasdfasdfadsfasdfasdhjghjhhgjghfasdfasdfaasdfasdfaadsfasdfadfasdfadfasdfasdfasdf" + System.currentTimeMillis()));
                            put.addColumn(Bytes.toBytes("base"), Bytes.toBytes("name"),
                                    Bytes.toBytes("yohoasdfasdfasdfasdfasdfasdfsdfasdfasfasdfasdfasdfasdfaadsfadfagfhfghfghfghsdfadsfasdfsdfadfasdfasdf" + System.currentTimeMillis()));
                            put.addColumn(Bytes.toBytes("base"), Bytes.toBytes("address"),
                                    Bytes.toBytes("nanjingasdfasdfasdfadfasdfasdfadfasdfasdghjgjghjghjfasdfasdfadfasdfasdfasdfadsfadfadfasdfasdfasdfasdfadfadfasdfasdfadfa" + System.currentTimeMillis()));
                            list.add(put);

                            if(list.size()>160){
                                table.put(list);
                                list = Lists.newArrayList();
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }

        executorService.shutdown();

    }

}

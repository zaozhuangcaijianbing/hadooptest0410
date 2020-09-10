package com.wn.spark;

import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Iterator;

import static org.apache.spark.sql.functions.col;

public class SparkSqlTest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .master("local[4]")
                .getOrCreate();

        //分区读取mysql数据
        Dataset<Row> jdbcDF = spark.read()
                .format("jdbc")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://10.142.98.225:2171/QDAM_CONFIG_DEMO?useUnicode=true&characterEncoding=utf8&autoReconnect=true&rewriteBatchedStatements=TRUE")
                .option("dbtable", "wnmysql0904")
//                .option("query","select test1,test2,test3 from wnmysql0904 limit 10000")  //dbtable和query不能同时设置
                .option("user", "QDAM_CONFIG_DEMO")
                .option("password", "3b9a06826b008e71")
                .option("numPartitions", 4)
                .option("partitionColumn", "id")
                .option("lowerBound", 1)
                .option("upperBound", 5000000)
                .load();

//        Dataset<Row> select = jdbcDF.filter(col("test2").equalTo("Bob"));

        //写入mysql，方式1
        jdbcDF.write()
                .format("jdbc")
                .option("driver","com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://10.142.98.225:2171/QDAM_CONFIG_DEMO?useUnicode=true&characterEncoding=utf8&autoReconnect=true&rewriteBatchedStatements=TRUE")
                .option("dbtable", "wnmysql0905")
                .option("user", "QDAM_CONFIG_DEMO")
                .option("password", "3b9a06826b008e71")
                .mode(SaveMode.Overwrite)
                .save();

        //写入mysql，方式2
//        jdbcDF.foreachPartition(new ForeachPartitionFunction<Row>() {
//
//            @Override
//            public void call(Iterator<Row> t) throws Exception {
//                Class.forName("com.mysql.jdbc.Driver");
//                Connection conn = DriverManager.getConnection("jdbc:mysql://10.142.98.225:2171/QDAM_CONFIG_DEMO?useUnicode=true&characterEncoding=utf8&autoReconnect=true&rewriteBatchedStatements=TRUE", "QDAM_CONFIG_DEMO", "3b9a06826b008e71");
//                conn.setAutoCommit(false);
//                PreparedStatement cmd = conn.prepareStatement("insert into wnmysql0905 (test1,test2,test3) values (?,?,?)");
//                long start = System.currentTimeMillis() / 1000;
//                while (t.hasNext()) {
//                    Row next = t.next();
//                    String test1 = next.getString(0);
//                    String test2 = next.getString(1);
//                    String test3 = next.getString(2);
//
//                    cmd.setString(1, test1);
//                    cmd.setString(2, test2);
//                    cmd.setString(3, test3);
//                    cmd.addBatch();
//                }
//                cmd.executeBatch();
//                conn.commit();
//                long end = System.currentTimeMillis() / 1000;
//                System.out.println("耗时：" + (end - start));
//            }
//        });


    }
}

package com.wn.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

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
                .option("driver","com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://10.142.98.225:2171/QDAM_CONFIG_DEMO?useUnicode=true&characterEncoding=utf8&autoReconnect=true&rewriteBatchedStatements=TRUE")
                .option("dbtable", "wnmysql0904")
                .option("user", "QDAM_CONFIG_DEMO")
                .option("password", "3b9a06826b008e71")
                .option("numPartitions",4)
                .option("partitionColumn","id")
                .option("lowerBound",1)
                .option("upperBound",5000000)
                .load();

//        Dataset<Row> select = jdbcDF.filter(col("test2").equalTo("Bob"));
//        Dataset<Row> cache = jdbcDF.cache();

        jdbcDF.write()
                .format("jdbc")
                .option("driver","com.mysql.jdbc.Driver")
                .option("url", "jdbc:mysql://10.142.98.225:2171/QDAM_CONFIG_DEMO?useUnicode=true&characterEncoding=utf8&autoReconnect=true&rewriteBatchedStatements=TRUE")
                .option("dbtable", "wnmysql0905")
                .option("user", "QDAM_CONFIG_DEMO")
                .option("password", "3b9a06826b008e71")
                .mode(SaveMode.Overwrite)
                .save();


        long count = 0L;
        System.out.println("count:" + count);
    }
}

package com.wn.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkTestExecutor {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkTestExecutor").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> stringJavaRDD = sc.textFile("F:\\hadooptest0410\\src\\main\\resources\\clickflow\\click.txt");

        long count = stringJavaRDD.count();
        System.out.println(count);
    }
}

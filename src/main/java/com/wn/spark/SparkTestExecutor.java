package com.wn.spark;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;

public class SparkTestExecutor {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkTestExecutor").setMaster("local[4]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> stringJavaRDD = sc.textFile("file:///Users/wangning/ideaProject/mytestproject/hadooptest0410/src/main/resources/clickflow/click.txt", 4);

        JavaRDD<KafkaMessage> map = stringJavaRDD.map(oldMessage -> {
            KafkaMessage kafkaMessage = JSON.parseObject(oldMessage, KafkaMessage.class);
            Thread.sleep(5);
            return kafkaMessage;
        });


        JavaRDD<KafkaMessage> iosFilter = map.filter(message -> {
            if ("yohobuy_ios".equals(message.getAk())) {
                return true;
            }
            return false;
        });

        JavaRDD<KafkaMessage> androidFilter = map.filter(message -> {
            if ("yohobuy_android".equals(message.getAk())) {
                return true;
            }
            return false;
        });

        JavaRDD<KafkaMessage> union = iosFilter.union(androidFilter);

        System.out.println("count1：" + union.count());

        JavaPairRDD<String, KafkaMessage> stringKafkaMessageJavaPairRDD = union.mapToPair(message -> {
            Thread.sleep(5);
            return new Tuple2<>(message.getAk(), message);
        });


        JavaPairRDD<String, Iterable<KafkaMessage>> stringIterableJavaPairRDD = stringKafkaMessageJavaPairRDD.groupByKey();


        long count = stringIterableJavaPairRDD.count();
        System.out.println("count2：" + count);
    }
}

package com.wn.spark;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;

import java.util.*;


/**
 * 使用spark-streaming-kafka-0-10
 * nohup spark-submit --master yarn --deploy-mode cluster --class com.wn.spark.SparkTestExecutor --executor-memory 2g --driver-memory 1g --num-executors 2 --executor-cores 2 --queue default --conf spark.default.parallelism=3 --conf spark.streaming.concurrentJobs=1 --conf spark.streaming.kafka.maxRatePerPartition=2000 /home/dd/example/hadoop-examples-jar-with-dependencies.jar >> /home/dd/example/example.log 2>&1 &
 * nohup spark-submit --master yarn --deploy-mode client --class com.wn.spark.SparkTestExecutor --executor-memory 2g --driver-memory 1g --num-executors 2 --executor-cores 2 --queue default --conf spark.default.parallelism=3 --conf spark.streaming.concurrentJobs=1 --conf spark.streaming.kafka.maxRatePerPartition=2000 /home/dd/example/hadoop-examples-jar-with-dependencies.jar >> /home/dd/example/example.log 2>&1 &
 */
public class SparkTestExecutor {
    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setAppName("SparkTestExecutor").set("spark.streaming.stopGracefullyOnShutdown", "true").setMaster("local[4]");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(20));


        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "server3:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "spark.emr.SparkTestExecutor");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("kafka_topic_test");

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );

        //打印本次条数
        stream.count().print();

        //Obtaining Offsets
        stream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
            @Override
            public void call(JavaRDD<ConsumerRecord<String, String>> consumerRecordJavaRDD) throws Exception {
                OffsetRange[] offsetRanges = ((HasOffsetRanges) consumerRecordJavaRDD.rdd()).offsetRanges();
                consumerRecordJavaRDD.foreachPartition(new VoidFunction<Iterator<ConsumerRecord<String, String>>>() {
                    @Override
                    public void call(Iterator<ConsumerRecord<String, String>> consumerRecordIterator) throws Exception {
                        OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
                        System.out.println(
                                o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
                    }
                });
            }
        });


        //提交偏移量
        stream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
            @Override
            public void call(JavaRDD<ConsumerRecord<String, String>> consumerRecordJavaRDD) throws Exception {
                OffsetRange[] offsetRanges = ((HasOffsetRanges) consumerRecordJavaRDD.rdd()).offsetRanges();
                ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges);
            }
        });


        streamingContext.start();
        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
            streamingContext.stop(true, true);
        }
    }
}

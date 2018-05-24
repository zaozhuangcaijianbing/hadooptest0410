package com.wn.spark;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

import java.util.*;


public class SparkStreamingTestExecutor2 {
    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingTestExecutor2").set("spark.streaming.stopGracefullyOnShutdown", "true").setMaster("local[4]");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(20));


        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "server3:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "spark.emr.SparkStreamingTestExecutor2");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("kafka_topic_test");

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );


        JavaDStream<KafkaMessage> map = stream.map(record -> {
            KafkaMessage kafkaMessage = JSON.parseObject(record.value(), KafkaMessage.class);
            return kafkaMessage;
        });

        JavaPairDStream<String, KafkaMessage> pairDStream = map.mapToPair(message -> {
            return new Tuple2<>(message.getAv(), message);
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

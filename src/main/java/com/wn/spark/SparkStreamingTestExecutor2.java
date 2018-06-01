package com.wn.spark;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.apache.spark.streaming.scheduler.*;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


public class SparkStreamingTestExecutor2 {
    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setAppName("SparkStreamingTestExecutor2").set("spark.streaming.stopGracefullyOnShutdown", "true").setMaster("local[4]");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        streamingContext.addStreamingListener(new StreamingListener() {
            private AtomicInteger totalAlarm=new AtomicInteger(0);

            private  int threshold = 2*10*1000;

            @Override
            public void onBatchStarted(StreamingListenerBatchStarted batchStarted) {
            }

            @Override
            public void onReceiverStarted(StreamingListenerReceiverStarted receiverStarted) {
            }

            @Override
            public void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {
                if(null!=batchCompleted.batchInfo()){
                    Long totalDelay=Long.valueOf(batchCompleted.batchInfo().totalDelay().get().toString());
                    Long processingDelay=Long.valueOf(batchCompleted.batchInfo().processingDelay().get().toString());
                    System.out.println(totalDelay);
                    System.out.println(processingDelay);
                }
            }

            @Override
            public void onReceiverStopped(StreamingListenerReceiverStopped receiverStopped) {
            }

            @Override
            public void onOutputOperationStarted(StreamingListenerOutputOperationStarted outputOperationStarted) {
            }

            @Override
            public void onBatchSubmitted(StreamingListenerBatchSubmitted batchSubmitted) {
            }

            @Override
            public void onOutputOperationCompleted(StreamingListenerOutputOperationCompleted outputOperationCompleted) {
            }

            @Override
            public void onReceiverError(StreamingListenerReceiverError receiverError) {
            }
        });


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

        pairDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, KafkaMessage>>() {
            @Override
            public void call(JavaPairRDD<String, KafkaMessage> stringKafkaMessageJavaPairRDD) throws Exception {
                stringKafkaMessageJavaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, KafkaMessage>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, KafkaMessage>> tuple2Iterator) throws Exception {
                        Thread.sleep(10000);
                    }
                });
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

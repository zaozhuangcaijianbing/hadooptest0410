package com.wn.spark;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Sets;
import kafka.serializer.StringDecoder;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.net.InetAddress;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;


/**
 * nohup spark-submit --master yarn --deploy-mode cluster --class com.wn.spark.SparkTestExecutor --executor-memory 2g --driver-memory 1g --num-executors 2 --executor-cores 2 --queue default --conf spark.default.parallelism=3 --conf spark.streaming.concurrentJobs=1 --conf spark.streaming.kafka.maxRatePerPartition=2000 /home/dd/example/hadoop-examples-jar-with-dependencies.jar >> /home/dd/example/example.log 2>&1 &
 * nohup spark-submit --master yarn --deploy-mode client --class com.wn.spark.SparkTestExecutor --executor-memory 2g --driver-memory 1g --num-executors 2 --executor-cores 2 --queue default --conf spark.default.parallelism=3 --conf spark.streaming.concurrentJobs=1 --conf spark.streaming.kafka.maxRatePerPartition=2000 /home/dd/example/hadoop-examples-jar-with-dependencies.jar >> /home/dd/example/example.log 2>&1 &
 */
public class SparkTestExecutor {
    public static void main(String[] args) throws Exception{
        System.out.println("SparkTestExecutor start: " + new Date() + ":" + InetAddress.getLocalHost().getHostAddress());

        SparkConf sparkConf = new SparkConf().setAppName("SparkTestExecutor").set("spark.streaming.stopGracefullyOnShutdown", "true");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(60));

        Map<String, String> kafkaParam = new KafkaParamBuilder().brokerAddress().group("spark.emr.SparkTestExecutor").toBuilder();

        JavaPairInputDStream<String, String> kafkaStream = KafkaUtils.createDirectStream(streamingContext, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParam, Sets.newHashSet("wn_0413"));

        System.out.println("SparkTestExecutor kafkaStream: " + new Date() + ":" + InetAddress.getLocalHost().getHostAddress());

        JavaDStream<KafkaMessage> messageRDD =  kafkaStream.map(new Function<Tuple2<String,String>, KafkaMessage>() {
            @Override
            public KafkaMessage call(Tuple2<String, String> tuple2) throws Exception {
                if (StringUtils.isBlank(tuple2._2)) {
                    return null;
                }

                System.out.println("SparkTestExecutor map: " + new Date() + ":" + InetAddress.getLocalHost().getHostAddress());

                KafkaMessage kafkaMessage = JSON.parseObject(tuple2._2, KafkaMessage.class);
                return kafkaMessage;
            }
        });

        JavaDStream<KafkaMessage> filterMessageRDD = messageRDD.filter(new Function<KafkaMessage, Boolean>() {

            @Override
            public Boolean call(KafkaMessage kafkaMessage) throws Exception {
                if (null == kafkaMessage || null == kafkaMessage.getOp()) {
                    return false;
                }

                System.out.println("SparkTestExecutor filter: " + new Date() + ":" + InetAddress.getLocalHost().getHostAddress());

                return true;
            }
        });


        JavaPairDStream<String, Integer> javaPairDStream = filterMessageRDD.mapToPair(new PairFunction<KafkaMessage, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(KafkaMessage message) throws Exception {
                return new Tuple2<>(message.getAk() + ":" + message.getUdid(), 1);
            }
        });

        JavaPairDStream<String, Integer> reduceRdd = javaPairDStream.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                System.out.println("SparkTestExecutor reduceByKey: " + v1 + ":" + new Date() + ":" + InetAddress.getLocalHost().getHostAddress());
                return v1 + v2;
            }
        });



        reduceRdd.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> stringIntegerJavaPairRDD) throws Exception {
                System.out.println("SparkTestExecutor foreachRDD: " +":" + new Date() + ":" + InetAddress.getLocalHost().getHostAddress());

                stringIntegerJavaPairRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> tuple2Iterator) throws Exception {
                        System.out.println("SparkTestExecutor foreachPartition: " +":" + new Date() + ":" + InetAddress.getLocalHost().getHostAddress());
                        while(tuple2Iterator.hasNext()){
                            Tuple2<String, Integer> next = tuple2Iterator.next();
                            System.out.println("SparkTestExecutor foreachPartition: "+":" + new Date() + ":" + next._1 + ":" + next._2);
                        }


                    }
                });
            }
        });

        System.out.println("SparkTestExecutor ttttttt: " + new Date() + ":" + InetAddress.getLocalHost().getHostAddress());

        streamingContext.start();
        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
            streamingContext.stop(true, true);
        }
    }
}

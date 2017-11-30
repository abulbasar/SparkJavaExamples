package com.example;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

public class KafkaStreaming {
	public static void main(String[] args) throws InterruptedException{
		
		SparkConf conf = new SparkConf()
		.setAppName(StreamingApp.class.getName())
		.setIfMissing("spark.master", "local[*]");
		
		JavaSparkContext sc = new JavaSparkContext(conf);

        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(5));
        
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("group.id", "spark_id");
        kafkaParams.put("auto.offset.reset", "largest");
        kafkaParams.put("enable.auto.commit", "false");
        
        Set<String> topics = new HashSet<String>();
        topics.add("demo");
        
        JavaPairInputDStream<String, String> stream =  KafkaUtils.createDirectStream(ssc, String.class, String.class, 
        		StringDecoder.class, StringDecoder.class, kafkaParams, topics);
        
        stream.print();
        
        ssc.start();
        ssc.awaitTermination();
		
		
	}
}

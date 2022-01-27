package com.example.streaming;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

/*
 * Set the project JDK version to 1.8 for lambda expressions to work.
 */

public class KafkaDirectStreaming {
	public static void main(String[] args) throws InterruptedException {

	    String topicName = "demo";
	    String appName = KafkaDirectStreaming.class.getName();

		SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setIfMissing("spark.master", "local[*]");

		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaStreamingContext ssc = new JavaStreamingContext(sc,
				Durations.seconds(5));

		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("group.id", "spark_streaming");

		// auto.offset.reset is valid only if no valid offset is found
		kafkaParams.put("auto.offset.reset", "largest");

		Set<String> topics = Collections.singleton(topicName);

		JavaPairInputDStream<String, String> stream = KafkaUtils
				.createDirectStream(ssc, String.class, String.class,
						StringDecoder.class, StringDecoder.class, kafkaParams,
						topics);

		JavaDStream<String> values = stream.map(p ->  p._2);

		values.print();

		//values.saveAsTextFiles("raw/data", "");

		ssc.start();
		ssc.awaitTermination();

	}
}

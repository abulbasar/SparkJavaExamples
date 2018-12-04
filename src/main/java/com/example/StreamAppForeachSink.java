package com.example;

import java.util.ArrayList;
import java.util.List;

import com.example.CustomForEachSink;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.helper.*;

public class StreamAppForeachSink {

	private static SparkSession spark = null;

	public static void main(String[] args) throws AnalysisException,
			StreamingQueryException {

		String checkpointDir = args[0];
		
		

		SparkConf conf = new SparkConf().setAppName(StreamAppForeachSink.class.getName())
				.setIfMissing("spark.master", "local[*]")
				.setIfMissing("spark.sql.shuffle.partitions", "2")
				.setIfMissing("spark.sql.streaming.checkpointLocation", checkpointDir);

		spark = SparkSession.builder().config(conf).getOrCreate();

		Dataset<String> rawStream = spark.readStream().format("socket")
				.option("host", "localhost").option("port", "9999").load()
				.as(Encoders.STRING());
	
		rawStream.printSchema();
		System.out.println("Is streaming: " + rawStream.isStreaming());
		
		
		
		Dataset<Row> tweets = rawStream
		.select(functions.from_json(functions.col("value")
				, Encoders.bean(Tweet.class).schema()).as("root"))
		.selectExpr("root.*")
		.withColumn("timestamp", functions.current_timestamp());
		
		System.out.println("Schema of tweets streaming dataset");
		tweets.printSchema();

		/*
		 ObjectMapper mapper = new ObjectMapper(); 
		 Dataset<Row> tweets = rawStream.mapPartitions(
				items -> {
					List<Tweet> tweetsIters = new ArrayList<Tweet>();
					while (items.hasNext()) {
						try {
							tweetsIters.add(mapper.readValue(items.next(),
									Tweet.class));
						} catch (Exception ex) {
							ex.printStackTrace();
						}
					}
					return tweetsIters.iterator();
				}, Encoders.bean(Tweet.class)).withColumn("timestamp",
				functions.current_timestamp());*/

		
		Dataset<Row> tagsAgg = tweets.withColumn(
				"tag",
				functions.explode(functions.split(functions.col("text"), "\\s+")))
				.filter(functions.substring(functions.col("tag"), 0, 1).equalTo("#"))
				.withWatermark("timestamp", "30 seconds")
				.groupBy(functions.col("tag"))
				.count();


		tagsAgg.writeStream()
				.outputMode(OutputMode.Update())
				.trigger(Trigger.ProcessingTime("5 seconds"))
				.foreach(new CustomForEachSink())
				.start();

		spark.streams().awaitAnyTermination();
	}
}


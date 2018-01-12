package com.example;

import java.util.ArrayList;
import java.util.List;

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

public class StructuredStreamTweets {

	private static SparkSession spark = null;

	public static void main(String[] args) throws AnalysisException,
			StreamingQueryException {
		ObjectMapper mapper = new ObjectMapper();

		SparkConf conf = new SparkConf().setAppName(StructuredStreamTweets.class.getName())
				.setIfMissing("spark.master", "local[*]")
				.setIfMissing("spark.sql.shuffle.partitions", "2");

		spark = SparkSession.builder().config(conf).getOrCreate();

		Dataset<String> rawStream = spark.readStream().format("socket")
				.option("host", "localhost").option("port", "9999").load()
				.as(Encoders.STRING());

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
				functions.current_timestamp());

		System.out.println("Is streaming: " + rawStream.isStreaming());
		tweets.printSchema();

		tweets.withColumn(
				"tag",
				functions.explode(functions.split(functions.col("text"), "\\s+")))
				.filter(functions.substring(functions.col("tag"), 0, 1)
						.equalTo("#"))
				.groupBy(
						functions.window(functions.col("timestamp"),
								"30 seconds", "10 seconds"),
						functions.col("tag")).count().writeStream()
				.outputMode(OutputMode.Update())
				.trigger(Trigger.ProcessingTime("5 seconds")).format("console")
				.option("truncate", false).option("numRows", 1000).start()
				.awaitTermination();
	}
}

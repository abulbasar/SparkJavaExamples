package com.example.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.example.helper.*;

/*
Start streaming source
$ cd ~/Downloads
$ java Stream datasets/tweets.json 9999 1
*/


public class StructuredStreamTweets {

	private static SparkSession spark = null;

	public static void main(String[] args) throws AnalysisException,
			StreamingQueryException {
		ObjectMapper mapper = new ObjectMapper();

		SparkConf conf = new SparkConf().setAppName(StructuredStreamTweets.class.getName())
				.setIfMissing("spark.master", "local[*]")
				.setIfMissing("spark.sql.shuffle.partitions", "2");

		spark = SparkSession.builder().config(conf).getOrCreate();

		Dataset<String> rawStream = spark
                .readStream()
                .format("socket")
				.option("host", "localhost")
                .option("port", "9999")
                .load()
				.as(Encoders.STRING());

        System.out.println("Is streaming: " + rawStream.isStreaming());

		Dataset<Tweet> tweets = rawStream.map((MapFunction<String, Tweet>) record ->
                mapper.readValue(record, Tweet.class), Encoders.bean(Tweet.class));

		tweets.printSchema();

		tweets
                .withColumn("timestamp", functions.current_timestamp())
                .withColumn("tag", functions.explode(functions.split(functions.col("text"), "\\s+")))
                .filter(functions.substring(functions.col("tag"), 0, 1).equalTo("#"))
                .groupBy(
                        functions.window(
                                functions.col("timestamp"),
                                "30 seconds"
                                , "10 seconds"
                            ),
                        functions.col("tag"))
                .count()
                .writeStream()
                .outputMode(OutputMode.Update())
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .format("console")
                .option("truncate", false)
                .option("numRows", 1000).start()
                .awaitTermination();
	}
}

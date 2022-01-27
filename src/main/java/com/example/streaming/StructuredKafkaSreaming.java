package com.example.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class StructuredKafkaSreaming {

	public static void main(String[] args) throws StreamingQueryException {

		String topicName = "demo";

		SparkConf conf = new SparkConf()
			.setAppName(StructuredKafkaSreaming.class.getName())
			.setIfMissing("spark.master", "local[*]");
		
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		
		Dataset<Row> stream = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", topicName)
                .load();
		
		stream.printSchema();

		
		StreamingQuery queryStream = stream
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "partition", "offset")
                .writeStream()
                .format("console")
                .start();

        spark.streams().awaitAnyTermination();
		spark.close();
		
	}

}

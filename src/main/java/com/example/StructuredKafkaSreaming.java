package com.example;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class StructuredKafkaSreaming {

	public static void main(String[] args) throws StreamingQueryException {
		SparkConf conf = new SparkConf()
			.setAppName(StructuredSreaming.class.getName())
			.setIfMissing("spark.master", "local[*]");
		
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		
		Dataset<Row> stream = spark
		.readStream()
		  .format("kafka")
		  .option("kafka.bootstrap.servers", "localhost:9092")
		  .option("subscribe", "demo")
		  .load();
		
		stream.printSchema();
		
		
		stream
		.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "partition", "offset")
		.writeStream()
		.format("console")
		.start()
		.awaitTermination();
		
		spark.close();
		
	}

}

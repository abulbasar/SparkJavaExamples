package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/*
* Learning Objectives:
* 1. "Hello World" of Spark application
* 2. How to build a basic Spark application
* 3. Adding logging mechanism and control logs
* 4. Saving the RDD to file system
*
*
* */

public final class WordCount {
	private static final Pattern SPACE = Pattern.compile("\\W+");
	private static Logger logger = LoggerFactory.getLogger(WordCount.class);

	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.err.println("Usage: com.example.WordCount <file> [<output path>]");
			System.exit(1);
		}

		logger.info("Input path: " + args[0]);

		SparkConf sparkConf = new SparkConf().setAppName("WordCount (using Java)");

		// Allow spark to overwrite the output on an existing directory
		sparkConf.set("spark.hadoop.validateOutputSpecs", "false");

		// If master is not passed, Spark application will set the master to local[*]
		sparkConf.setIfMissing("spark.master", "local[*]");

		logger.info("Creating the spark context");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = sc.textFile(args[0], 1);

		JavaRDD<String> words = lines.flatMap((String s) -> {
			return Arrays.asList(SPACE.split(s)).iterator();
		});

		JavaPairRDD<String, Integer> ones = words.mapToPair((String s) -> {
			return new Tuple2<String, Integer>(s, 1);
		});

		JavaPairRDD<String, Integer> counts = ones.reduceByKey((Integer i1, Integer i2) -> {
			return i1 + i2;

		});

		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<String, Integer> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}

		if (args.length >= 2) {
			logger.info("Saving the output to: " + args[1]);
			counts.saveAsTextFile(args[1]);
		}

		System.out.println("Process is complete.");
		sc.stop();
		sc.close();
	}
}
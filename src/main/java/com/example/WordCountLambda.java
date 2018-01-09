package com.example;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;


/*
* Learning Objectives:
* 1. "Hello World" of Spark application using Java Lambda expression
* 2. Specify JDK version 1.8 in Maven to support Lambda expression
*
*
* */


public final class WordCountLambda {
    private static final Pattern SPACE = Pattern.compile("\\W+");
    private static Logger logger = LoggerFactory.getLogger(WordCountLambda.class);

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: com.example.WordCountLambda <file> [<output path>]");
            System.exit(1);
        }

        logger.info("Input path: " + args[0]);

        SparkConf sparkConf = new SparkConf().setAppName("WordCount (using Java Lambda)");

        //Allow spark to overwrite the output on an existing directory
        sparkConf.set("spark.hadoop.validateOutputSpecs", "false");


        //If master is not passed, Spark application will set the master to local[*]
        sparkConf.setIfMissing("spark.master", "local[*]");

        logger.info("Creating the spark context");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = sc.textFile(args[0], 1);

        JavaRDD<String> words = lines.flatMap((s) -> Arrays.asList(SPACE.split(s.toLowerCase())).iterator());

        JavaPairRDD<String, Integer> ones = words.mapToPair(word -> new Tuple2<>(word, 1));

        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

        List<Tuple2<String, Integer>> output = counts.collect();

        for (Tuple2<String,Integer> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }

        if(args.length >=2) {
            logger.info("Saving the output to: " +  args[1]);
            counts.saveAsTextFile(args[1]);
        }

        System.out.println("Process is complete.");
        sc.stop();
        sc.close();

    }
}

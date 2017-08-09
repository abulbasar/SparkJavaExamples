package com.example;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Map;


/*
* Learning Objectives:
* 1. How to create a PairRDD
* 2. Use byKey functions of pairRDD
* 3. Save data of Pair RDD
* 4. Use collectAsMap on PairRDD to get all values of RDD to the driver
*
* */


final public class StockAnalyzer {

    private final static Logger logger = LoggerFactory.getLogger(StockAnalyzer.class);


    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: com.example.StockAnalyzer <input> <output>");
            System.exit(-1);
        }
        SparkConf sparkConf = new SparkConf().setAppName("Stock Analyzer");
        sparkConf.setIfMissing("spark.master", "local[*]");

        // By default spark does not overwrite existing dir while writing the output.
        // The property value below allows spark to overwrite an exiting dir.
        sparkConf.set("spark.hadoop.validateOutputSpecs", "false");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        logger.info(sparkConf.toDebugString());


        JavaRDD<String> stocksRdd = sc.textFile(args[0], 1);

        for (String r : stocksRdd.take(10)) {
            System.out.println(r);
        }

        JavaRDD<String> stocksData = stocksRdd.filter(s -> !s.toLowerCase().startsWith("date"));

        System.out.println("Stocks data after filtering out header");

        for (String r : stocksData.take(10)) {
            System.out.println(r);
        }

        JavaPairRDD<String, Double> symbolVolumeRdd = stocksData.mapToPair(s -> {
            String[] tokens = s.split(",");
            return new Tuple2<String, Double>(tokens[7], Double.valueOf(tokens[5]));
        });

        //Group the data by symbol value
        JavaPairRDD<String, Iterable<Double>> symbolGrouped = symbolVolumeRdd.groupByKey();


        JavaPairRDD<String, Double> symbolAvgVolume = symbolGrouped.mapValues(values -> {
            Double sum = 0.0;
            int count = 0;
            for (Double r : values) {
                sum += r;
                ++count;
            }
            return sum / count;
        });

        System.out.println("\n\nAverage volume per stock symbol...");

        Map<String, Double> symbolAvgVolumeMap = symbolAvgVolume.collectAsMap();
        for (String key : symbolAvgVolumeMap.keySet()) {
            System.out.println(key + ": " + symbolAvgVolumeMap.get(key));
        }

        System.out.println("\n\nSaving the stock average volume to filesystem ..." + args[1]);
        symbolAvgVolume.saveAsTextFile(args[1]);
        System.out.println("Process has been complete");

        sc.stop();
        sc.close();

    }


}

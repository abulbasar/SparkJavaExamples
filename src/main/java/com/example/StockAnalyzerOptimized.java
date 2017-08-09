package com.example;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


final public class StockAnalyzerOptimized {


    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: com.example.StockAnalyzerOptimized <input> <output>");
            System.exit(-1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("StockAnalyzerOptimized");
        sparkConf.setIfMissing("spark.master", "local[*]");


        //Allow spark to overwrite the output on an existing directory
        sparkConf.set("spark.hadoop.validateOutputSpecs", "false");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> stocksRdd = sc.textFile(args[0], 1);

        for (String r : stocksRdd.take(10)) {
            System.out.println(r);
        }

        JavaRDD<String> stocksData = stocksRdd.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                return !s.startsWith("date");
            }
        });

        System.out.println("Stocks data after removing header");

        for (String r : stocksData.take(10)) {
            System.out.println(r);
        }

        JavaPairRDD<String, Tuple2<Double, Integer>> symbolVolumeRdd = stocksData.mapToPair(new PairFunction<String, String, Tuple2<Double, Integer>>() {

            public Tuple2<String, Tuple2<Double, Integer>> call(String s) {
                String[] tokens = s.split(",");
                Tuple2<Double, Integer> volumeOne = new Tuple2<Double, Integer>(Double.parseDouble(tokens[5]), 1);

                return new Tuple2<String, Tuple2<Double, Integer>>(tokens[7], volumeOne);
            }
        });

        JavaPairRDD<String, Tuple2<Double, Integer>> symVolSumCount = symbolVolumeRdd.reduceByKey(new Function2<Tuple2<Double, Integer>, Tuple2<Double, Integer>, Tuple2<Double, Integer>>() {
            public Tuple2<Double, Integer> call(Tuple2<Double, Integer> pair1, Tuple2<Double, Integer> pair2) throws Exception {
                return new Tuple2<Double, Integer>(pair1._1 + pair2._1, pair1._2 + pair2._2);
            }
        });

        System.out.println("Output of Symbol Volume and sum of count");
        for (Tuple2<String, Tuple2<Double, Integer>> r : symVolSumCount.take(10)) {
            System.out.println(r._1 + ": " + r._2._1 + " " + r._2._2);
        }

        JavaPairRDD<String, Double> symVolAvg = symVolSumCount.mapValues(new Function<Tuple2<Double, Integer>, Double>() {

            public Double call(Tuple2<Double, Integer> r) {
                return r._2 / r._1;
            }
        });


        System.out.print("Saving the avg volume for each stock symbol");
        symVolAvg.saveAsTextFile(args[1]);


        System.out.print("Process is complete. Press <enter> key to exit.");

        System.in.read();
        sc.stop();
        sc.close();


    }


}

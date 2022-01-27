package com.example.rdd.internal;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;

public class ShuffleBehaviourApp {
    transient SparkConf sparkConf;
    transient JavaSparkContext sparkContext;

    private void pause(String step){
        System.out.println("Stopped at: " + step);
        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Exiting step: " + step);
    }

    public void init(){
        sparkConf = new SparkConf()
                .setAppName(getClass().getName())
                .setIfMissing("spark.default.parallelism", "8")
                .setIfMissing("spark.master", "local[*]");
        sparkContext = new JavaSparkContext(sparkConf);
        System.out.println("Spark UI: " + sparkContext.sc().uiWebUrl());
    }

    public Namespace parseArguments(String[] args) throws Exception {
        final ArgumentParser parser = ArgumentParsers
                .newFor(getClass().getName()).build()
                .description("Stock application using java");

        parser.addArgument("-i", "--input")
                .help("Path for movies file")
                .required(true);

        Namespace res = parser.parseArgs(args);
        return res;
    }

    static void println(Object object){
        System.out.println(object);
    }


    private void start(String[] args) throws Exception {
        final Namespace namespace = parseArguments(args);
        final String path = namespace.getString("input");

        init();

        JavaRDD<String> rdd = sparkContext.textFile(path);

        final JavaPairRDD<String, Double> pairRdd = rdd
                .filter(line -> line.startsWith("2016"))
                .map(line -> line.split(","))
                .mapToPair(tokens -> new Tuple2<>(tokens[7], Double.valueOf(tokens[5])));

        // Find maximum volume for each stock
        List<Tuple2<String, Double>> result = pairRdd
                .groupByKey()
                .mapValues(values -> {
                    double max = 0.0;
                    for (Double value : values) {
                        max = Math.max(max, value);
                    }
                    return max;
                })
                .collect(); // Shuffle data : 664.8 KB
        println("Output from group by key: " + result);
        result = pairRdd
                .reduceByKey((a, b) -> Math.max(a, b))
                .collect();// Shuffle data 6.6 KB
        println("Output from reduce by key: " + result);

        pause("End");
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Spark application");
        final ShuffleBehaviourApp app = new ShuffleBehaviourApp();
        app.start(args);
        app.close();
        System.out.println("Processing completed");
    }

    public void close() {
        sparkContext.close();
    }


}

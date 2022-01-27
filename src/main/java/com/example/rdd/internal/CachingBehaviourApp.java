package com.example.rdd.internal;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;

public class CachingBehaviourApp {
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

        //rdd.cache();
        //rdd.persist(StorageLevel.MEMORY_ONLY());
        // Cache the data in java object
        // processing is faster because no time is spent on deserialization/deserialization
        // but takes more memory

        //rdd.persist(StorageLevel.MEMORY_ONLY_SER()); // Cache the data in bytes

        rdd.persist(StorageLevel.DISK_ONLY());
        // keep the cache data on disks
        // use it when reading from slow data sources of data such as s3 or rdbms
        // use it if you do not have enough memory to cache in memory

        final long startTime = System.currentTimeMillis();
        int STEPS = 10;
        for (int i = 0; i < STEPS; i++) {
            final JavaPairRDD<String, Iterable<Double>> groupedRdd = rdd
                    .filter(line -> line.startsWith("2016"))
                    .map(line -> line.split(","))
                    .mapToPair(tokens -> new Tuple2<>(tokens[7], Double.valueOf(tokens[5])))
                    .groupByKey(13);

            final JavaPairRDD<String, Double> avgVolumeRdd = groupedRdd
                    .mapValues(values -> {
                        double sum = 0.0;
                        double count = 0;
                        for (Double value : values) {
                            sum += value;
                            ++count;
                        }
                        return sum / count;
                    });
            final List<Tuple2<String, Double>> avgVolumePerSymbol =
                    avgVolumeRdd.collect();

        }
        final long durationPerStep = (System.currentTimeMillis() - startTime) / STEPS;
        println("Avg time taken for aggregation: " + durationPerStep);
        pause("End");
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Spark application");
        final CachingBehaviourApp app = new CachingBehaviourApp();
        app.start(args);
        app.close();
        System.out.println("Processing completed");
    }

    public void close() {
        sparkContext.close();
    }


}

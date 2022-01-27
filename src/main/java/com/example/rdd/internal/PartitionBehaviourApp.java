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

public class PartitionBehaviourApp {
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

        JavaRDD<String> rdd = sparkContext.textFile(path, 5);
        // if the input files are gzip the minPartitions parameter is ignored
        // otherwise, number of partitions = max(natural partition count, minPartitions)
        // natural partition count = number of blocks of the file if file is on HDFS
        println("Number of partitions in rdd: " + rdd.getNumPartitions());


        rdd = rdd.repartition(7);
        // increase the number of partition
        // repartition also randomly shuffles the data, which removes
        // any data distribution skew in the partitions
        // repartition is a wide operation, hence it adds a stage to the DAG

        println("Number of partitions in rdd after repartition: " + rdd.getNumPartitions());

        rdd = rdd.coalesce(3);
        // decrease the number of partition
        // coalesce is a narrow operation
        println("Number of partitions in rdd after coalesce: " + rdd.getNumPartitions());



        println("Number of records: " + rdd.count());
                //.map(line -> line.split(","))
                ;
        println(rdd.take(10));

        final JavaPairRDD<String, Iterable<Double>> groupedRdd = rdd
                .filter(line -> line.startsWith("2016"))
                .map(line -> line.split(","))
                .mapToPair(tokens -> new Tuple2<>(tokens[7], Double.valueOf(tokens[5])))
                .groupByKey(13);

        println("Number of partitions of the groupedByRdd: " + groupedRdd.getNumPartitions());

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
        final List<Tuple2<String, Double>> avgVolumePerSymbol = avgVolumeRdd.collect();

        println("Number of partitions of avgVolumeRdd: " + avgVolumeRdd.getNumPartitions());
        println("Id of avgVolumeRdd: " + avgVolumeRdd.id());

        println(avgVolumePerSymbol);
        pause("Start");
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Spark application");
        final PartitionBehaviourApp app = new PartitionBehaviourApp();
        app.start(args);
        app.close();
        System.out.println("Processing completed");
    }

    public void close() {
        sparkContext.close();
    }


}

package com.example.dataframe;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import scala.Option;

import java.io.Closeable;
import java.io.IOException;

import static com.example.common.Utils.println;

public class PartitioningApp implements Closeable {

    // Dataset for twitter: https://drive.google.com/drive/folders/1QAJ3Kbz56xu5m4OW34fJDI5S74yPtIKX?usp=sharing

    private SparkConf sparkConf;
    private SparkSession sparkSession;


    public static void main(String[] args) throws Exception {
        final long startTime = System.currentTimeMillis();
        final PartitioningApp app = new PartitioningApp();
        app.start(args);
        app.close();
        final long duration = System.currentTimeMillis() - startTime;
        println(String.format("Process is complete. Took: %d secs", duration/1000));
    }

    private void start(String[] args) throws Exception {
        final Namespace namespace = parseArguments(args);
        init();

        String inputPath = namespace.getString("input");

        ///////////////////////// Partitioning and Bucketing ////////////////////////////

        Dataset<Row> dataset = sparkSession.read()
                .format("csv")
                .option("inferSchema", "true")
                .option("sampleRatio", "0.001")
                .option("header", "true")
                .load(inputPath)
                .withColumn("year", functions.expr("year(date)"))
                .withColumn("month", functions.expr("month(date)"))
                .withColumn("day", functions.expr("day(date)"))
                ;

        dataset.printSchema();
        dataset.show();

        dataset
                .selectExpr("avg(volume)")
                .show();

        dataset
                .filter("year = 2015")
                .selectExpr("avg(volume)")
                .show();

        String partitionPath = "/tmp/stocks_partitioned";

        dataset
                .write()
                .partitionBy("year")
                .mode(SaveMode.Overwrite)
                .format("json")
                .save(partitionPath);

        dataset = sparkSession
                .read()
                .format("json")
                .load(partitionPath);

        dataset
                .filter("year = 2015")
                .selectExpr("avg(volume)")
                .show();

        System.in.read();
    }

    private void init() {
        final SparkConf sparkConf = new SparkConf()
                .setAppName(getClass().getName())
                .setIfMissing("spark.master", "local[*]");

        sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        final Option<String> webUrl = sparkSession.sparkContext().uiWebUrl();
        println("Spark UI: " + webUrl);
    }

    public Namespace parseArguments(String[] args) throws Exception {
        final ArgumentParser parser = ArgumentParsers
                .newFor(getClass().getName()).build()
                .description("Twitter application using java");

        parser.addArgument("-i", "--input")
                .help("Path for input file")
                .required(true);

        Namespace res = parser.parseArgs(args);
        return res;
    }

    @Override
    public void close() throws IOException {
        sparkSession.close();
    }

}

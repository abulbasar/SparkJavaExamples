package com.example.df;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class StockAnalysis {


    public void start(String ... args) throws Exception{

        if (args.length != 2) {
            System.out.println("Provide input and output path ");
            System.exit(1);
        }

        final String inputPath = args[0];
        final String outputPath = args[1];

        SparkConf conf = new SparkConf();
        conf.setAppName(StockAnalysis.class.getName());
        conf.setIfMissing("spark.master", "local[*]");
        conf.setIfMissing("spark.default.parallelism", "16");
        conf.setIfMissing("spark.sql.shuffle.partitions", "3");
        conf.setIfMissing("spark.driver.memory", "4G");
        //conf.setIfMissing("spark.executor.memory", "4G");
        conf.set("spark.hadoop.validateOutputSpecs", "false");


        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        final SparkContext sparkContext = spark.sparkContext();
        final JavaSparkContext sc = new JavaSparkContext(sparkContext);

        final Dataset<Row> stocks = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv(inputPath);

        stocks.write().mode("overwrite").save(outputPath);



        spark.close();

    }

    public static void main(String ... args) throws Exception {
        new StockAnalysis().start(args);
    }
}

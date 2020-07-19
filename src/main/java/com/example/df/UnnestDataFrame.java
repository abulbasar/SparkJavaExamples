package com.example.df;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class UnnestDataFrame {
    public void start(String ... args) throws Exception{

        if (args.length != 1) {
            System.out.println("Provide input path ");
            System.exit(1);
        }

        final String inputPath = args[0];

        SparkConf conf = new SparkConf();
        conf.setAppName(StockAnalysis.class.getName());
        conf.setIfMissing("spark.master", "local[*]");


        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        final Dataset<Row> df = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv(inputPath);

        df.show();


        spark.close();

    }

    public static void main(String ... args) throws Exception {
        new UnnestDataFrame().start(args);
    }
}

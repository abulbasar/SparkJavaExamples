package com.example.df;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HiveTableAccess {


    public void start(String ... args) throws Exception{

        if (args.length != 2) {
            System.out.println("Provide input and output path ");
            System.exit(1);
        }

        final String inputPath = args[0];
        final String outputTableName = args[1];

        SparkConf conf = new SparkConf();
        conf.setAppName(HiveTableAccess.class.getName());
        conf.setIfMissing("spark.master", "local[*]");
        conf.setIfMissing("spark.default.parallelism", "16");
        conf.setIfMissing("spark.sql.shuffle.partitions", "3");
        conf.setIfMissing("spark.driver.memory", "4G");
        //conf.setIfMissing("spark.executor.memory", "4G");
        conf.set("spark.hadoop.validateOutputSpecs", "false");


        SparkSession spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();

        final Dataset<Row> stocks = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv(inputPath);

        spark.sql("show tables").show(10, false);

        stocks.createOrReplaceTempView("stocks_temp");

        stocks.write().mode("append").saveAsTable(outputTableName);

        spark.sql("show tables").show(10, false);

        spark.close();

    }

    public static void main(String ... args) throws Exception {
        new HiveTableAccess().start(args);
    }
}

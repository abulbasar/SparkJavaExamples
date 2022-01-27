package com.example.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.Option;

import java.io.Closeable;
import java.io.IOException;

import static com.example.common.Utils.println;

public class PartitioningAndBucketingApp implements Closeable {

    // Dataset for twitter: https://drive.google.com/drive/folders/1QAJ3Kbz56xu5m4OW34fJDI5S74yPtIKX?usp=sharing

    private SparkConf sparkConf;
    private SparkSession sparkSession;


    public static void main(String[] args) throws Exception {
        final long startTime = System.currentTimeMillis();
        final PartitioningAndBucketingApp app = new PartitioningAndBucketingApp();
        app.start(args);
        app.close();
        final long duration = System.currentTimeMillis() - startTime;
        println(String.format("Process is complete. Took: %d secs", duration/1000));
    }

    private void start(String[] args) throws Exception {

        init();

        String partitionPath = "/tmp/stocks_partitioned";

        Dataset<Row> dataset = sparkSession
                .read()
                .format("json")
                .load(partitionPath);

        dataset.write()
                .mode(SaveMode.Overwrite)
                .partitionBy("year")
                .bucketBy(10, "symbol")
                .saveAsTable("stocks");

        //dataset.createOrReplaceTempView("stocks");
        dataset.show();
        dataset.printSchema();
        sparkSession.sql("MSCK REPAIR TABLE stocks");

        sparkSession.sql("select avg(volume) from stocks" +
                " where year = 2015 and symbol = 'GE'").show();

//        dataset
//                .filter("year = 2015")
//                .selectExpr("avg(volume)")
//                .show();

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

    @Override
    public void close() throws IOException {
        sparkSession.close();
    }

}

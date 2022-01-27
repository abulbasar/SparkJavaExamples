package com.example.dataframe;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public abstract class SparkBaseApp {
    JavaSparkContext sc;
    SparkConf sparkConf;
    SparkSession sparkSession;

    long startTime;

    void initSparkSession(){
        startTime = System.currentTimeMillis();
        sparkConf = new SparkConf()
                .setAppName(getClass().getName())
                .setMaster("local[*]")
                .setIfMissing("spark.sql.shuffle.partitions", "10")
        ;

        sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        sc = new JavaSparkContext(sparkSession.sparkContext());

        println("Spark Web UI: " + sc.sc().uiWebUrl());
    }

    void println(Object object){
        System.out.println(object);
    }

    void show(JavaRDD<?> rdd){
        show(rdd, 10);
    }
    void show(JavaRDD<?> rdd, int count){
        println("Showing the records from rdd: " + rdd.id());
        rdd.take(count).stream().forEach(System.out::println);
        println(String.format("Showing %d of %d records", count, rdd.count()));
    }

    static CsvParser getCsvParser(){
        // Univocity parser is a library for csv data parsing
        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator("\n");
        return new CsvParser(settings);
    }

    void pause(String stepName) throws IOException {
        println(String.format("Pausing for step: %s. Press any key to continue.", stepName));
        System.in.read();
        println(String.format("Stepping over %s", stepName));
    }

    void close(){
        sparkSession.close();
        final long duration = (System.currentTimeMillis()-startTime)/ 1_000;
        println(String.format("Process is complete. Closing Spark session. Duration: %dsec", duration) );
    }
}

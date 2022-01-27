package com.example.rdd;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

public abstract class SparkBaseApp {
    JavaSparkContext sc;
    SparkConf sparkConf;

    void initSparkSession(){
        sparkConf = new SparkConf()
                .setAppName(getClass().getName())
                .setMaster("local");
        sc = new JavaSparkContext(sparkConf);
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

    static Integer countIterable(Iterable<?> iterable){
        int count = 0;
        for (Object o : iterable) {
            ++count;
        }
        return count;
    }
}

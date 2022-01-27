package com.example.common;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;

public class Utils {
    public static void println(Object object){
        System.out.println(object);
    }

    public void show(JavaRDD<?> rdd){
        show(rdd, 10);
    }
    public void show(JavaRDD<?> rdd, int count){
        println("Showing the records from rdd: " + rdd.id());
        rdd.take(count).stream().forEach(System.out::println);
        println(String.format("Showing %d of %d records", count, rdd.count()));
    }

    public static CsvParser getCsvParser(){
        // Univocity parser is a library for csv data parsing
        CsvParserSettings settings = new CsvParserSettings();
        settings.getFormat().setLineSeparator("\n");
        return new CsvParser(settings);
    }

    public void pause(String stepName) throws IOException {
        println(String.format("Pausing for step: %s. Press any key to continue.", stepName));
        System.in.read();
        println(String.format("Stepping over %s", stepName));
    }

    public static Integer sizeIterable(Iterable<?> iterable){
        int count = 0;
        for (Object o : iterable) {
            ++count;
        }
        return count;
    }

}

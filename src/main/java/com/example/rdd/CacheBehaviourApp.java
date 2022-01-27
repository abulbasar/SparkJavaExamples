package com.example.rdd;

import com.univocity.parsers.csv.CsvParser;
import lombok.Data;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;

import java.io.Serializable;

public class CacheBehaviourApp extends SparkBaseApp{

    @Data
    public static class Stock implements Serializable {
        private String date;
        private Double open; private Double high;
        private Double low;
        private Double close;
        private Double volume;
        private Double adjclose;
        private String symbol;
    }

    private static Double toDouble(String s){
        try{
            return Double.valueOf(s);
        }catch (Exception e){
            return null;
        }
    }

    private static Stock toStock(String line){
        final CsvParser csvParser = getCsvParser();
        final String[] tokens = csvParser.parseLine(line);
        return toStock(tokens);
    }

    private static Stock toStock(String[] tokens){
        final Stock stock = new Stock();
        stock.setDate(tokens[0]);
        stock.setOpen(toDouble(tokens[1]));
        stock.setClose(toDouble(tokens[2]));
        stock.setHigh(toDouble(tokens[3]));
        stock.setLow(toDouble(tokens[4]));
        stock.setVolume(toDouble(tokens[5]));
        stock.setAdjclose(toDouble(tokens[6]));
        stock.setSymbol(tokens[7]);
        return stock;
    }

    @Data
    public static class StockStats implements Serializable{
        private String symbol;
        private Double avgReturn;
        private Integer count;
    }

    public Namespace parseArguments(String[] args) throws ArgumentParserException {
        final ArgumentParser parser = ArgumentParsers.newFor("prog").build()
                .description("My Spark application for movie lens dataset");

        parser.addArgument("-i", "--input-file")
                .help("Path for stocks file").required(true);
        Namespace res = parser.parseArgs(args);
        return res;
    }



    public void start(String[] args) throws Exception {
        final long startTime = System.currentTimeMillis();
        final Namespace namespace = parseArguments(args);
        final String inputFile = namespace.getString("input_file");
        initSparkSession();

        // Base RDD: does not have any parent
        final JavaRDD<String> rawRdd = sc.textFile(inputFile, 7).map(String::toUpperCase); //.sample(false, 0.01);
        println("Number of partitions for rawRdd: " + rawRdd.getNumPartitions());
        println("----------------- Convert each line to Stock class object -----------------------------");

        //rawRdd.persist(StorageLevel.MEMORY_ONLY()); // default
        // caches the rdd and keeps it on disk rather than memory
        rawRdd.persist(StorageLevel.DISK_ONLY()); // default
        rawRdd.count();

//        final JavaRDD<String> upperCaseRdd = rawRdd
//                .map(String::toUpperCase)
//                .persist(StorageLevel.MEMORY_AND_DISK());
//        upperCaseRdd.count();

        pause("Unpersist");

        rawRdd.unpersist();// removes the rdd from cache

        pause("Completion");
    }

    public static void main(String[] args) throws Exception {

        new CacheBehaviourApp().start(args);
    }
}

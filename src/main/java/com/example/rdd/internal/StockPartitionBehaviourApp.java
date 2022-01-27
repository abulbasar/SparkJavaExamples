package com.example.rdd.internal;

import lombok.AllArgsConstructor;
import lombok.Data;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.List;

public class StockPartitionBehaviourApp implements Serializable{


    public static void println(Object... objects){
        for (Object object : objects) {
            System.out.print(object);
        }
        System.out.println();
    }

    public static void main(String[] args) throws Exception {
        System.out.println("My spark application ...");
        final StockPartitionBehaviourApp app = new StockPartitionBehaviourApp();
        app.start(args);
    }

    private Namespace parseArguments(String[] args) throws ArgumentParserException {
        ArgumentParser parser = ArgumentParsers.newFor("StockApp")
                .build()
                .description("My Stock processing application using java streaming api");
        parser.addArgument("-f", "--file")
                .required(true)
                .help("Input file path for stocks.csv");
        Namespace res = parser.parseArgs(args);
        return res;
    }

    @Data
    public static class Stock implements Serializable {
        String date;
        Double open;
        Double high;
        Double low;
        Double close;
        Double volume;
        Double adjclose;
        String symbol;

        public String getWeekDay(){
            return LocalDate.parse(date).getDayOfWeek().toString();
        }

        public Double getDailyReturn(){
            return 100 * (close - open)/open;
        }
    }

    private Stock toSock(String line){
        final String[] tokens = line.split(",");
        //date,open,high,low,close,volume,adjclose,symbol
        Stock stock = new Stock();
        stock.setDate(tokens[0]);
        stock.setOpen(Double.parseDouble(tokens[1]));
        stock.setHigh(Double.parseDouble(tokens[2]));
        stock.setLow(Double.parseDouble(tokens[3]));
        stock.setClose(Double.parseDouble(tokens[4]));
        stock.setVolume(Double.parseDouble(tokens[5]));
        stock.setAdjclose(Double.parseDouble(tokens[6]));
        stock.setSymbol(tokens[7]);
        return stock;
    }

    @Data
    public static class StockStats implements Serializable{
        private String symbol;
        private Integer count;
        private Double avgVolume;
    }

    @Data
    @AllArgsConstructor
    public static class Pair implements Serializable{
        private String key;
        private Long value;
    }

    private void start(String[] args) throws Exception{
        final long startTime = System.currentTimeMillis();

        final Namespace namespace = parseArguments(args);
        final String file = namespace.getString("file");

        final SparkConf sparkConf = new SparkConf()
                .setAppName(getClass().getName())
                .setIfMissing("spark.master", "local[*]")
                .setIfMissing("spark.default.parallelism", "10")
                ;
        final JavaSparkContext sc = new JavaSparkContext(sparkConf);

        final JavaRDD<String> rdd = sc
                .textFile(file)
                .repartition(3)
                ;

        println("Number of partitions of rdd variable: " + rdd.getNumPartitions());
        println("Number of records: " + rdd.count()); // Action 1

        final JavaRDD<Stock> stockRDD = rdd
                .filter(line -> !(line.isEmpty() || line.startsWith("date")))
                .map(this::toSock);

        final JavaPairRDD<String, Double> pairRDD = stockRDD
                .mapToPair(stock -> new Tuple2<>(stock.getSymbol(), stock.getVolume()));

        final JavaPairRDD<String, Iterable<Double>> grouped = pairRDD.groupByKey(1);
        println("Number of partitions on the grouped Rdd: " + grouped.getNumPartitions());

        println("----- Stock Stats ----");
        final List<StockStats> stockStats = grouped
                .map(pair -> {
                    final String symbol = pair._1();
                    final Iterable<Double> volumes = pair._2;
                    double sum = 0.0;
                    int count = 0;
                    for (Double volume : volumes) {
                        sum += volume;
                        ++count;
                    }
                    final StockStats stats = new StockStats();
                    stats.setAvgVolume(sum / count);
                    stats.setCount(count);
                    stats.setSymbol(symbol);
                    return stats;
                })
                //.sortBy(com.example.rdd.StockApp.StockStats::getAvgVolume, false, 1)
                .collect() // Action 2
                ;

        println("Stocks stats: ", stockStats);

        println("Time taken: " + (System.currentTimeMillis()-startTime));


        Thread.sleep(1000000);

    }

}

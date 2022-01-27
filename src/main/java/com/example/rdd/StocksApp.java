package com.example.rdd;

import com.univocity.parsers.csv.CsvParser;
import lombok.Data;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

public class StocksApp extends SparkBaseApp{

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
        final JavaRDD<String> rawRdd = sc.textFile(inputFile); //.sample(false, 0.01);
        println("----------------- RAW rdd -----------------------------");
        show(rawRdd);

        println("----------------- Convert each line to Stock class object -----------------------------");
        /*
        final JavaRDD<Stock> stockJavaRDD = rawRdd
                .filter(line -> !line.startsWith("date"))
                .map(StocksApp::toStock)
                .cache();
        show(stockJavaRDD);
        */

        //pause("Prior_StockClass_Conversion");


        final JavaRDD<Stock> stockJavaRDD = rawRdd
                .mapPartitions(iterator -> {
                    final CsvParser csvParser = getCsvParser();
                    final List<Stock> batch = new LinkedList<>();
                    while (iterator.hasNext()){
                        final String line = iterator.next();
                        if(!line.startsWith("date")) {
                            final Stock stock = toStock(csvParser.parseLine(line));
                            batch.add(stock);
                        }
                    }
                    return batch.iterator();
                }).cache();
        show(stockJavaRDD);

        println("-----------------How many records are there in 2016 -----------------------------");
        long count = rawRdd.filter(line -> line.startsWith("2016")).count();
        println("Number of records: " + count);


        println("-----------------How many records are there in 2016 for stock INTC -----------------------------");
        final JavaRDD<Stock> intc2016 = stockJavaRDD.filter(rec ->
                rec.getDate().startsWith("2016") && "INTC".equals(rec.getSymbol()));
        count = intc2016.count();
        println("Number of records: " + count);

        println("----------------- Percent days for positive gain for INTC in 2016 -----------------------------");
        final long countPositiveGain = intc2016.filter(rec -> rec.getClose() > rec.getOpen()).count();
        println(String.format("Percentage: %.2f", countPositiveGain * 100.0/count));

        println("----------------- Top stocks based on volume 2016 -----------------------------");
        final JavaRDD<Stock> stocks2016 = stockJavaRDD.filter(rec -> rec.getDate().startsWith("2016")).cache();
        final List<Stock> top3ByVolume = stocks2016.sortBy(Stock::getVolume, false, 1).take(3);
        top3ByVolume.stream()
                .map(rec -> String.format("%s -> %.0f", rec.getSymbol(), rec.getVolume()))
                .forEach(this::println);


        println("----------------- Avg volume and count per symbol in 2016 -----------------------------");
        final JavaPairRDD<Double, StockStats> topStocksByAvgReturn = stocks2016.groupBy(Stock::getSymbol)
                .map(tuple -> {
                    final String symbol = tuple._1();
                    final Iterable<Stock> batch = tuple._2();
                    double sum = 0.0;
                    int batchCount = 0;
                    for (Stock stock : batch) {
                        final Double open = stock.getOpen();
                        final Double close = stock.getClose();
                        final double dailyReturn = 100 * (close - open) / open;
                        ++batchCount;
                        sum += dailyReturn;
                    }
                    final double avg = sum / batchCount;
                    final StockStats stats = new StockStats();
                    stats.setAvgReturn(avg);
                    stats.setCount(batchCount);
                    stats.setSymbol(symbol);
                    return stats;
                })
                .keyBy(StockStats::getAvgReturn)
                .sortByKey(false);

        topStocksByAvgReturn.map(Tuple2::_2).take(3).forEach(this::println);

        println("----------------- Find return per stock in 2016 -----------------------------");
        final JavaPairRDD<Double, String> returnByStockIn2016 = stocks2016.groupBy(Stock::getSymbol)
                .mapValues((Iterable<Stock> batch) -> {
                    List<Stock> list = new ArrayList<>();
                    batch.forEach(list::add);
                    list.sort(Comparator.comparing(Stock::getDate));
                    final Stock first = list.get(0);
                    final Stock last = list.get(list.size() - 1);
                    return 100 * (last.getAdjclose() - first.getAdjclose()) / first.getAdjclose();
                })
                .mapToPair(t -> new Tuple2<>(t._2, t._1))
                .sortByKey(false);

        returnByStockIn2016.take(3).stream()
                .map(t -> String.format("%s -> %.2f", t._2, t._1))
                .forEach(this::println);

        println("----------------- Maximum daily return across all stocks in  2016 -----------------------------");
        final JavaDoubleRDD dailyReturnRdd = stocks2016.mapToDouble(r -> (r.getClose() - r.getOpen()) / r.getOpen());
        println(String.format("Maximum: %.2f, Minimum: %.2f", dailyReturnRdd.max(), dailyReturnRdd.min()));

        final StatCounter statCounter = dailyReturnRdd.stats();
        println("Std: " + statCounter.stdev());
        println("Min: " + statCounter.min());
        println("Max: " + statCounter.max());
        println("Count: " + statCounter.count());

        println(String.format("Process is complete. Taken: %d millis", System.currentTimeMillis()-startTime));
        //pause("Completion");
    }

    public static void main(String[] args) throws Exception {

        new StocksApp().start(args);
    }
}

package com.example.rdd.internal;

import lombok.Data;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;


/*
* Problem statement: Find number of the of stocks record that belongs the stocks in the watchlist
* Using accumulator we can save additional jobs.
*
* */
public class BroadcastAndAccumulator implements Serializable {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Data
    public class Stock implements Serializable {
        private Date date;
        private Double open;
        private Double high;
        private Double low;
        private Double close;
        private Double volume;
        private Double adjclose;
        private String symbol;
    }

    private Namespace parseArguments(String[] args) throws ArgumentParserException {
        ArgumentParser parser = ArgumentParsers.newFor("StockApp")
                .build()
                .description("Spark demo application");
        parser.addArgument("-f", "--file")
                .required(true)
                .help("Input file path for stocks.csv");
        Namespace res = parser.parseArgs(args);
        return res;
    }

    public void start(String[] args) throws Exception {

        final Namespace namespace = parseArguments(args);
        final String inputPath = namespace.getString("file");
        System.out.println("Input path: " + inputPath);

        SparkConf conf = new SparkConf();
        conf.setAppName(getClass().getName());
        conf.setMaster("local[4]");
        conf.setIfMissing("spark.master", "local[4]");
        conf.setIfMissing("spark.default.parallelism", "16");
        conf.set("spark.hadoop.validateOutputSpecs", "false");

        JavaSparkContext sc = new JavaSparkContext(conf);

        System.out.println("Spark Web UI: " + sc.sc().uiWebUrl());

        final JavaRDD<String> rdd = sc.textFile(inputPath);

        final List<String> watchListStocks = Arrays.asList("GE", "FB", "INTC", "AAPL");

        final JavaRDD<String> rdd2016 = rdd.filter(line -> line.startsWith("2016"));

        /*
        * We could perform these partition operation using normal RDD operations like filter followed by a count
        * But, utilizing accumulator we can reduce one extra job for this purpose
        * Accumulator can be read only in the driver but cannot be read in the executor,
        * which mean you cannot read the value inside map, forech etc. function.
        *
        * Use of broadcast variable reduced the dependency of the shared data on the driver.
        * */
        final LongAccumulator positiveReturnCount2016 = sc.sc().longAccumulator();
        final LongAccumulator countOfWatchedStock = sc.sc().longAccumulator();

        final Broadcast<List<String>> broadcastWhitelistStocks = sc.broadcast(watchListStocks);
        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        final JavaRDD<Stock> stockRdd = rdd2016.map(line -> {
            final String[] split = line.split(",");
            final Stock stock = new Stock();
            stock.setSymbol(split[7]);
            stock.setClose(Double.valueOf(split[4]));
            stock.setDate(new Date(simpleDateFormat.parse(split[0]).getTime()));
            stock.setHigh(Double.valueOf(split[2]));
            stock.setLow(Double.valueOf(split[3]));
            stock.setVolume(Double.valueOf(split[5]));
            stock.setOpen(Double.valueOf(split[1]));

            boolean positiveReturn = stock.getOpen() < stock.getClose();

            if(positiveReturn) {
                positiveReturnCount2016.add(1);
            }

            final List<String> whitelistStocksValue = broadcastWhitelistStocks.getValue();
            if(whitelistStocksValue.contains(stock.getSymbol())){
                countOfWatchedStock.add(1);
            }

            return stock;
        });


        final List<Stock> list = stockRdd.collect();
        System.out.println("Number of records: " + list.size());

        System.out.println("Positive return count in 2016: " + positiveReturnCount2016.value());
        System.out.println("Number of stocks for watch list: " + countOfWatchedStock.value());





        System.out.println("Completed. Waiting for termination.");

        Thread.sleep(120000);

        sc.close();

    }

    public static void main(String ... args) throws Exception {
        new BroadcastAndAccumulator().start(args);
    }
}

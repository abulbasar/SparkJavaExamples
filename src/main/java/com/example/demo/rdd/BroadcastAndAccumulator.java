package com.example.rdd;

import com.example.models.AvgVolumeByStock;
import com.example.models.Stock;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.Accumulator;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;

public class BroadcastAndAccumulator implements Serializable {

    private Logger logger = LoggerFactory.getLogger(getClass());

    BroadcastAndAccumulator(){

    }

    private void show(JavaRDD<?> rdd){
        System.out.println("Number of partitions: " + rdd.partitions().size());
        final List<?> objects = rdd.take(10);
        for(int i=0;i<objects.size();++i){
            System.out.println(String.format("[%d]", i) + objects.get(i));
        }
    }

    public void start(){
        SparkConf conf = new SparkConf();
        conf.setAppName(getClass().getName());
        conf.setMaster("local[4]");
        conf.setIfMissing("spark.master", "local[4]");
        conf.setIfMissing("spark.default.parallelism", "16");
        conf.set("spark.hadoop.validateOutputSpecs", "false");

        JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<String> rdd = sc.textFile("/Users/abasar/data/stocks.csv.gz", 6);
        System.out.println(rdd.count());

        show(rdd);

        final JavaRDD<String> rdd2019 = rdd.filter(line -> line.startsWith("2019"));

        final Accumulator<Integer> positiveReturnCount2019 = sc.intAccumulator(0);
        final Accumulator<Integer> countOfWatchedStock = sc.intAccumulator(0);


        final List<String> watchListStocks = Arrays.asList("GE", "FB", "INTC", "AAPL");

        final Broadcast<List<String>> broadcastWhitelistStocks = sc.broadcast(watchListStocks);


        final JavaRDD<Stock> stockRdd = rdd2019.map(line -> {
            final String[] split = line.split(",");
            final Stock stock = new Stock();
            stock.setSymbol(split[7]);
            stock.setClose(Double.valueOf(split[4]));
            stock.setDate(LocalDate.parse(split[0]));
            stock.setHigh(Double.valueOf(split[2]));
            stock.setLow(Double.valueOf(split[3]));
            stock.setVolume(Double.valueOf(split[5]));
            stock.setOpen(Double.valueOf(split[1]));

            boolean positiveReturn = stock.getOpen() < stock.getClose();

            if(positiveReturn) {
                positiveReturnCount2019.add(1);
            }

            final List<String> whitelistStocksValue = broadcastWhitelistStocks.getValue();
            if(whitelistStocksValue.contains(stock.getSymbol())){
                countOfWatchedStock.add(1);
            }

            return stock;
        });


        stockRdd.saveAsTextFile("output");

        System.out.println("Positive return count in 2019: " + positiveReturnCount2019.value());
        System.out.println("Number of stocks for watch list: " + countOfWatchedStock.value());



        sc.close();

    }

    public static void main(String ... args){
        new BroadcastAndAccumulator().start();
    }
}

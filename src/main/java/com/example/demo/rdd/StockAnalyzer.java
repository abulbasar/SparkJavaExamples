package com.example.rdd;

import com.example.models.AvgVolumeByStock;
import com.example.models.Stock;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class StockAnalyzer implements Serializable {

    private Logger logger = LoggerFactory.getLogger(getClass());

    StockAnalyzer(){

    }

    private void show(String message, JavaRDD<?> rdd){
        System.out.println(message);
        show(rdd);
    }
    private void show(JavaRDD<?> rdd){
        System.out.println("Number of partitions: " + rdd.partitions().size());
        final List<?> objects = rdd.take(10);
        for(int i=0;i<objects.size();++i){
            System.out.println(String.format("[%d]", i) + objects.get(i));
        }
    }

    private void show(String message,  JavaPairRDD<?, ?> rdd){
        System.out.println(message);
        System.out.println("Number of partitions: " + rdd.partitions().size());
        final List<? extends Tuple2<?, ?>> objects = rdd.take(10);
        for(int i=0;i<objects.size();++i){
            System.out.println(String.format("[%d]", i) + objects.get(i));
        }
    }


    public void start(){
        SparkConf conf = new SparkConf();
        conf.setAppName(getClass().getName());
        conf.setMaster("local[4]"); // 4 executor threads
        //conf.set("spark.master", "local[4]");
        //conf.set("spark.master", "yarn");
        conf.setIfMissing("spark.master", "local[4]");
        conf.setIfMissing("spark.default.parallelism", "16");
        conf.set("spark.hadoop.validateOutputSpecs", "false");

        JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<String> rdd = sc.textFile("/Users/abasar/data/stocks.csv.gz", 6);
        System.out.println(rdd.count());

        //show(rdd);

        final JavaRDD<String> rdd2019 = rdd.filter(line -> line.startsWith("2019"));

        show("rdd2019", rdd2019);


        final JavaRDD<Stock> stockRdd = rdd2019.map(line -> {
            final String[] split = line.split(",");
            final Stock stock = new Stock();
            stock.setSymbol(split[7]);
            stock.setDate(LocalDate.parse(split[0]));
            stock.setHigh(Double.valueOf(split[2]));
            stock.setLow(Double.valueOf(split[3]));
            stock.setVolume(Double.valueOf(split[5]));
            return stock;
        });


        System.out.println("Stock RDD");
        show(stockRdd);

        final JavaRDD<Stock> stock2019XLNX = stockRdd.filter(r -> "XLNX".equals(r.getSymbol()));


        final Double maxHigh = stock2019XLNX.map(Stock::getHigh).reduce(Math::max);
        final Double minLow = stock2019XLNX.map(stock -> stock.getLow()).reduce((Double x, Double y) -> Math.min(x, y));

        System.out.println("XLNX Max High: " + maxHigh + " minLow: " + minLow);


        // Find avg volume by stock symbol in 2019
        // select symbol, avg(volume) from stock where year(date)=2019 group by symbol


        final JavaPairRDD<String, Double> volumeBySymbol = stockRdd.mapToPair(r ->
                new Tuple2<>(r.getSymbol(), r.getVolume()));

        final JavaPairRDD<String, Double> avgVolumeBySymbol = volumeBySymbol
                .groupByKey()
                .mapValues((Iterable<Double> values) -> {
                    double sum = 0;
                    int count = 0;
                    for (Double value : values) {
                        sum += value;
                        ++count;
                    }
                    return sum / count;
                })
                ;


        final List<Tuple2<String, Double>> take = avgVolumeBySymbol.take(10);
        for (Tuple2<String, Double> tuple : take) {
            System.out.println(tuple._1() + " " + tuple._2());
        }


        final JavaRDD<Tuple2<String, Double>> avgVolumeBySymbolSorted = avgVolumeBySymbol
                .map(r -> new Tuple2<>(r._1(), r._2()))
                .sortBy(tuple -> tuple._2(), false, 3);

        show("avgVolumeBySymbolSorted", avgVolumeBySymbolSorted);


        //avgVolumeBySymbol.saveAsTextFile("/tmp/avg-volume-by-symbol");


//
//        avgVolumeBySymbol.map(rec -> {
//            ObjectMapper  objectMapper = new ObjectMapper();
//            final ObjectNode objectNode = objectMapper.createObjectNode();
//            objectNode.put("symbol", rec._1());
//            objectNode.put("avg", rec._2());
//            return objectMapper.writeValueAsString(objectNode);
//        })
//        .saveAsTextFile("/tmp/avg-volume-by-symbol");



//        avgVolumeBySymbol.map(rec -> {
//            ObjectMapper  objectMapper = new ObjectMapper();
//            final AvgVolumeByStock avgVolumeByStock = new AvgVolumeByStock();
//            avgVolumeByStock.setSymbol(rec._1());
//            avgVolumeByStock.setVolume(rec._2());
//            return objectMapper.writeValueAsString(avgVolumeByStock);
//        }).saveAsTextFile("/tmp/avg-volume-by-symbol");


        System.out.println("Number of partitions for avgVolumeBySymbol: " + avgVolumeBySymbol.partitions().size());


        final int numPartitions = 5;

        final JavaPairRDD<String, Double> avgVolumeBySymbolPartitioned = avgVolumeBySymbol
                .partitionBy(new Partitioner() {
                    @Override
                    public int getPartition(Object key) {
                        int chr = ((String) key).toUpperCase().charAt(0) - 65;
                        return (chr / 6);
                    }

                    @Override
                    public int numPartitions() {
                        return numPartitions;
                    }
                });


        final JavaRDD<String> jsonAvgVolumeBySymbol =avgVolumeBySymbolPartitioned
                .mapPartitions((Iterator<Tuple2<String, Double>> partition) -> {
                    // Runs at the executor
                    ObjectMapper objectMapper = new ObjectMapper();
                    List<String> jsons = new ArrayList<>();
                    while (partition.hasNext()) {
                        final Tuple2<String, Double> rec = partition.next();
                        final AvgVolumeByStock avgVolumeByStock = new AvgVolumeByStock();
                        avgVolumeByStock.setSymbol(rec._1());
                        avgVolumeByStock.setVolume(rec._2());
                        final String json = objectMapper.writeValueAsString(avgVolumeByStock);
                        jsons.add(json);
                    }
                    return jsons.iterator();
                },  true);

        jsonAvgVolumeBySymbol
                .saveAsTextFile("/tmp/avg-volume-by-symbol");

        sc.close();

    }

    public static void main(String ... args){
        new StockAnalyzer().start();
    }
}

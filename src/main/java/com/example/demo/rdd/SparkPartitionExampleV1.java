package com.example.rdd;

import com.example.models.Stock;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.List;
import java.util.Scanner;

public class SparkPartitionExampleV1 implements Serializable {

    private Logger logger = LoggerFactory.getLogger(getClass());

    SparkPartitionExampleV1(){

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

    private void waitForUser(String message){
        System.out.println(message + " Press Enter");
        final Scanner scanner = new Scanner(System.in);
        final String nextLine = scanner.nextLine();
    }


    public void start(){
        SparkConf conf = new SparkConf();
        conf.setAppName(getClass().getName());
        conf.setMaster("local[4]");
        conf.setIfMissing("spark.master", "local[4]");
        conf.setIfMissing("spark.default.parallelism", "16");
        conf.set("spark.hadoop.validateOutputSpecs", "false");

        JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<String> rdd = sc
                .textFile("/Users/abasar/data/stocks.csv.gz", 6)
                .repartition(3)
                ;
        System.out.println(rdd.count());

        show(rdd);

        rdd.take(10);



        final JavaRDD<String> rdd2019 = rdd.filter(line -> line.startsWith("2019"));

        show("rdd2019", rdd2019);


        final JavaRDD<Stock> stockRdd = rdd2019
                .map(line -> {
                    final String[] split = line.split(",");
                    final Stock stock = new Stock();
                    stock.setSymbol(split[7]);
                    stock.setDate(LocalDate.parse(split[0]));
                    stock.setHigh(Double.valueOf(split[2]));
                    stock.setLow(Double.valueOf(split[3]));
                    stock.setVolume(Double.valueOf(split[5]));
                    return stock;
                });

        System.out.println("Number of partitions for stocksRdd: " + stockRdd.partitions().size());

        final int NUM_PARTITIONS = 7;

        final JavaPairRDD<String, Double> resultRdd = stockRdd
                .mapToPair(r ->
                        new Tuple2<>(r.getSymbol(), r.getVolume()))
                .groupByKey(NUM_PARTITIONS)
                .mapValues((Iterable<Double> values) -> {
                    double sum = 0;
                    int count = 0;
                    for (Double value : values) {
                        sum += value;
                        ++count;
                    }
                    return sum / count;
                });


        assert NUM_PARTITIONS == resultRdd.getNumPartitions();
        System.out.println("Number of partitions for resultRdd: " + resultRdd.partitions().size());

        resultRdd.collect();

        // textFile -> filter -> map -> mapToPair -> groupByKey -> mapValues => Action
        //




        waitForUser("Showing count");
        sc.close();

    }

    public static void main(String ... args){
        new SparkPartitionExampleV1().start();
    }
}

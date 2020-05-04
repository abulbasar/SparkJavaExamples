package com.example.rdd;

import com.example.models.Stock;
import com.example.models.StockVolume;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.time.LocalDate;
import java.util.List;
import java.util.Scanner;

public class SparkPartitionExampleV2 implements Serializable {


    /*

    Create HDFS directory for stocks
    hadoop fs -mkdir /user/root/stocks

    Upload stocks.csv.gz
    You can download stocks.csv.gz from https://drive.google.com/open?id=15_BF38a6IIFVbp27kypxlq1TaHf4eBlO
    hadoop fs -put stocks.csv.gz /user/root/stocks


    Submit spark application on YARN
    spark-submit \
    --class com.example.rdd.SparkPartitionExampleV2 \
    --master yarn \
    --deploy-mode client \
    --num-executors 2 \
    --executor-memory 1g \
    SparkExamplesDemo-1.0-SNAPSHOT.jar  \
    hdfs:///user/root/stocks /user/root/stocks-output

    * */

    private Logger logger = LoggerFactory.getLogger(getClass());

    SparkPartitionExampleV2(){

    }

    private void show(JavaRDD<?> rdd){
        System.out.println("Number of partitions: " + rdd.partitions().size());
        final List<?> objects = rdd.take(10);
        for(int i=0;i<objects.size();++i){
            System.out.println(String.format("[%d]", i) + objects.get(i));
        }
    }


    private void waitForUser(String message){
        System.out.println(message + " Press enter to continue ...");
        final Scanner scanner = new Scanner(System.in);
        final String nextLine = scanner.nextLine();
    }


    public void start(String ... args){

        if(args.length != 2){
            System.out.println("Provide input and output path ");
            System.exit(1);
        }

        final String inputPath = args[0];
        final String outputPath = args[1];

        SparkConf conf = new SparkConf();
        conf.setAppName(SparkPartitionExampleV2.class.getName());
        //conf.setMaster("local[4]");
        conf.setIfMissing("spark.master", "local[4]");
        conf.setIfMissing("spark.default.parallelism", "16");
        conf.setIfMissing("spark.driver.memory", "400M");
        conf.setIfMissing("spark.executor.memory", "1G");
        conf.set("spark.hadoop.validateOutputSpecs", "false");

        JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<String> rdd = sc
                .textFile(inputPath, 6)
                ;


        //logger.info("Spark Web UI: " + sc.);
        // Putting rdd in cache
        // rdd.cache();
        // Above Same as rdd.persist(StorageLevel.MEMORY_ONLY())

        rdd.persist(StorageLevel.MEMORY_ONLY_SER());
        rdd.count();

        //waitForUser("added to cache");

        final JavaRDD<String> rdd2019 = rdd.filter(line -> line.startsWith("2019"));

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

        logger.info("Number of partitions for stocksRdd: " + stockRdd.partitions().size());

        final JavaPairRDD<String, StockVolume> stockVolumeSummary = stockRdd
                .mapToPair(r -> new Tuple2<>(r.getSymbol(), new StockVolume(r.getVolume(), 1)))
                .reduceByKey(StockVolume::add);


        final JavaPairRDD<String, Double> resultRdd =  stockVolumeSummary
                .mapToPair((Tuple2<String, StockVolume> pair) ->  {
                    final String symbol = pair._1();
                    final StockVolume stockVolume = pair._2();
                    return  new Tuple2<>(symbol, 1.0 * stockVolume.getSum()/stockVolume.getCount());
                });


        logger.info("Number of partitions for resultRdd: " + resultRdd.partitions().size());

        resultRdd.collect();

        // textFile -> filter -> map -> mapToPair -> groupByKey -> mapValues => Action

        resultRdd.coalesce(1).saveAsTextFile(outputPath, GzipCodec.class);


        // Remove the RDD from cache. Not required to execute it explicitly.
        rdd.unpersist();

        waitForUser("Waiting to show spark memory management");

        sc.close();
        logger.info("Completed the task");

    }

    public static void main(String ... args){
        new SparkPartitionExampleV2().start(args);
    }
}

package com.example.rdd;

import com.example.models.AvgVolumeByStock;
import com.example.models.Stock;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Doubles;
import org.apache.commons.math3.util.MathUtils;
import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaSparkStatusTracker;
import org.apache.spark.scheduler.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class StockAnalyzerReduceByKey implements Serializable {

    private Logger logger = LoggerFactory.getLogger(getClass());

    StockAnalyzerReduceByKey(){

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


    public void start() throws IOException {
        SparkConf conf = new SparkConf();
        conf.setAppName(getClass().getName());
        conf.setMaster("local[4]"); // 4 executor threads
        //conf.set("spark.master", "local[4]");
        //conf.set("spark.master", "yarn");
        conf.setIfMissing("spark.master", "local[4]");
        conf.setIfMissing("spark.default.parallelism", "16");
        conf.set("spark.hadoop.validateOutputSpecs", "false");
        conf.set("spark.eventLog.enabled", "true");
        conf.set("spark.eventLog.dir", "/tmp/spark-events-logs");

        JavaSparkContext sc = new JavaSparkContext(conf);

        System.out.println("Spark UI: " + sc.sc().uiWebUrl().get());

        sc.sc().addSparkListener(new SparkListener() {


            @Override
            public void onJobEnd(SparkListenerJobEnd jobEnd) {
                super.onJobEnd(jobEnd);

            }

        });

        final JavaRDD<String> rdd = sc.textFile("/Users/abasar/data/stocks.csv.gz", 6);
        System.out.println(rdd.count());

        //show(rdd);

        final JavaSparkStatusTracker javaSparkStatusTracker = sc.statusTracker();
        final int[] activeJobIds = javaSparkStatusTracker.getActiveJobIds();
        for (int activeJobId : activeJobIds) {
            final SparkJobInfo sparkJobInfo = javaSparkStatusTracker.getJobInfo(activeJobId);
            final int[] stageIds = sparkJobInfo.stageIds();
            final JobExecutionStatus status = sparkJobInfo.status();
            for (int stageId : stageIds) {
                final SparkStageInfo sparkStageInfo = javaSparkStatusTracker.getStageInfo(stageId);
            }
        }


        final JavaRDD<String> rdd2019 = rdd.filter(line -> line.startsWith("2019"));

        show("rdd2019", rdd2019);

        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");


        final JavaRDD<Stock> stockRdd = rdd2019.map(line -> {
            final String[] split = line.split(",");
            final Stock stock = new Stock();
            stock.setSymbol(split[7]);
            stock.setDate(new Date(simpleDateFormat.parse(split[0]).getTime()));
            stock.setHigh(Double.valueOf(split[2]));
            stock.setLow(Double.valueOf(split[3]));
            stock.setVolume(Double.valueOf(split[5]));
            return stock;
        });


        final JavaPairRDD<String, Double> volatilityRdd = stockRdd.mapToPair(stock -> {
            final double volatility = (stock.getHigh() - stock.getLow()) / stock.getLow();
            return new Tuple2<>(stock.getSymbol(), volatility);
        });

//        JavaPairRDD<String, Double> volatility=  volatilityRdd.reduceByKey((Double v1, Double v2)
//                -> v1 != null && v2 != null ? Math.max(v1, v2) : null)
//                .mapToPair(t -> new Tuple2<>(t._2, t._1))
//                .sortByKey(true)
//                .mapToPair(t -> new Tuple2<>(t._2, t._1))
                ;


        JavaPairRDD<String, Double> volatility = volatilityRdd.groupByKey()
                .mapValues(iterator -> {
                    List<Double> values = new ArrayList<>();
                    for (Double aDouble : iterator) {
                        values.add(aDouble);
                    }
                    final double[] doubles = Doubles.toArray(values);
                    return Doubles.max(doubles);
                });


        System.out.println("Id of volatility: " + volatility.id());

        volatility.coalesce(3).saveAsTextFile("/tmp/volatility");

        System.out.println("Press any key to exit");
        //System.in.read();
        sc.close();

    }

    public static void main(String ... args) throws IOException {
        new StockAnalyzerReduceByKey().start();
    }
}

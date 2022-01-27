package com.example.dataframe;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class UdfApp {

    private transient SparkSession session;

    public static void main(String[] args) throws Exception{
        final UdfApp app = new UdfApp();
        app.start(args);
        System.out.println("Application is complete");
    }


    private SparkSession getSparkSession(){
        final SparkConf sparkConf = new SparkConf()
                .setAppName(getClass().getName())
                .setIfMissing("spark.master", "local[*]");

        final SparkSession session = SparkSession.builder().config(sparkConf).getOrCreate();

        final SparkContext sparkContext = session.sparkContext(); // Scala spark context
        System.out.println("Spark WebUI: " + sparkContext.uiWebUrl());
        final JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);
        // JavaSparkContext is required when you want to create RDD
        return session;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Stock implements Serializable{
        private String symbol;
        private Double open;
        private Double close;
    }




    private void start(String[] args) throws Exception{
        System.out.println("My DataFrame application");

        final SparkSession sparkSession = getSparkSession();
        final List<String> items = Arrays.asList("info@jpmc.com", "admin@google.com", null, "");

        final Dataset<String> dataset = sparkSession.createDataset(items, Encoders.STRING());

        dataset.show();

        sparkSession.udf().register("extract_domain", (String s)-> {
            if(s != null){
                final String[] tokens = s.split("@");
                if(tokens.length == 2){
                    return tokens[1];
                }
            }
            return null;
        }, DataTypes.StringType);


        dataset.withColumn("domain", functions.expr("extract_domain(value)")).show();


        final Dataset<Stock> stockDataset = sparkSession.createDataset(Arrays.asList(
                new Stock("GE", 10.6, 10.8),
                new Stock("GE", 10.3, 10.6),
                new Stock("GE", 10.6, 11.10),
                new Stock("GE", 10.6, 10.8)
        ), Encoders.bean(Stock.class));

        stockDataset.show();

        sparkSession.udf().register("pct", (Row r)-> {
            final int openIndex = r.fieldIndex("open");
            final int closeIndex = r.fieldIndex("close");
            final Double open = r.isNullAt(openIndex)?null:r.getDouble(openIndex);
            final Double close = r.isNullAt(closeIndex)?null:r.getDouble(closeIndex);

            if(open != null && close != null){
                return 100 * (close - open)/open;
            }
            return null;
        }, DataTypes.DoubleType);

        stockDataset.withColumn("return", functions.expr("pct(struct(*))")).show();

        sparkSession.udf().register("pct_v2", (Double open, Double close)-> {

            if(open != null && close != null){
                return 100 * (close - open)/open;
            }
            return null;
        }, DataTypes.DoubleType);

        stockDataset.withColumn("return", functions.expr("pct_v2(open, close)")).show();

        List<String> values = Arrays.asList(
                "Here are the top 10 most viewed #crypto coins for the continent of #NorthAmerica!!",
                null,
                "",
                "What crypto are you trading this weekend?"
        );

        // Define a udf which will count the number of hashtags
        // count_hash_tags:
        // @param String s:
        // @return Integer : count of the hash tags

        sparkSession.udf().register("count_hashtags", (String value) -> {
            int count = 0;
            if(value != null) {
                final String[] tokens = value.split(" ");
                for(String t: tokens){
                    if(t.startsWith("#")){
                        ++count;
                    }
                }
            }
            return count;
        }, DataTypes.IntegerType);

        sparkSession
                .createDataset(values, Encoders.STRING())
                .withColumn("count_hash_tags", functions.expr("count_hashtags(value)"))
                .show(10, false);
        ;


    }

}

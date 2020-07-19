package com.example.df;

import com.example.models.Stock;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Date;

public class DataFrameDemo {


    public void processStockDataset(Dataset<Stock> stock){
        ///

    }

    public void start(String ... args) throws Exception{

        if (args.length != 2) {
            System.out.println("Provide input and output path ");
            System.exit(1);
        }

        final String inputPath = args[0];
        final String outputPath = args[1];

        SparkConf conf = new SparkConf();
        conf.setAppName(DataFrameDemo.class.getName());
        //conf.setMaster("local[4]");
        conf.setIfMissing("spark.master", "local[*]");
        conf.setIfMissing("spark.default.parallelism", "16");
        conf.setIfMissing("spark.sql.shuffle.partitions", "3");
        conf.setIfMissing("spark.driver.memory", "4G");
        //conf.setIfMissing("spark.executor.memory", "4G");
        conf.set("spark.hadoop.validateOutputSpecs", "false");


        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        final SparkContext sparkContext = spark.sparkContext();
        final JavaSparkContext sc = new JavaSparkContext(sparkContext);

        final Dataset<Row> stocks = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv(inputPath);

        stocks.show(10, false);

        stocks.printSchema();

        stocks.createOrReplaceTempView("stocks_table");

        // Spark SQL
        final Dataset<Row> dataset = spark.sql("select symbol, avg(volume), min(adjclose), max(adjclose) " +
                "from stocks_table group by symbol");

        dataset.show(10, false);


        // Dataframe DSL
        final Dataset<Row> stocksSummary = stocks
                .groupBy("symbol")
                .agg(functions.avg("volume").as("avg_volume")
                        , functions.min("adjclose").alias("min_close")
                        , functions.max("adjclose").alias("max_close")
                );

        stocksSummary.show(10, false);


        stocks
                .withColumn("date_type1", functions.column("date").cast("date"))
                .withColumn("date_type2", functions.column("date").cast(DataTypes.DateType))
                .withColumn("week", functions.dayofweek(functions.col("date")))
                .withColumnRenamed("date", "timestamp")
                .drop("low")
                .show(10, false)
                ;

        spark.udf().register("get_week_name", (Date date)-> {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("E");
            return simpleDateFormat.format(date);
        }, DataTypes.StringType);


        stocks
                .withColumn("week_num", functions.expr("get_week_name(date)"))
                .show(10, false);


        //stocksSummary.write().mode(SaveMode.Overwrite).save("output"); // Default format for read/write is Parquet with Snappy compression

        //stocksSummary.write().mode(SaveMode.Overwrite).format("csv").option("header", true).save("output");

        stocksSummary.write().mode(SaveMode.Overwrite).format("json").option("header", true).save("output");





        final JavaRDD<String> rdd = sc
                .textFile(inputPath, 6)
                .filter(line -> !line.startsWith("date"))
                ;


        rdd.take(10);

        final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");


        final JavaRDD<Stock> stockRdd = rdd
                .map(line -> {
                    final String[] split = line.split(",");
                    final Stock stock = new Stock();
                    stock.setSymbol(split[7]);
                    stock.setDate(new java.sql.Date(simpleDateFormat.parse(split[0]).getTime()));
                    stock.setHigh(Double.valueOf(split[2]));
                    stock.setLow(Double.valueOf(split[3]));
                    stock.setVolume(Double.valueOf(split[5]));
                    return stock;
                });


        final Dataset<Stock> stockRddConvertedToDataset = spark.createDataset(stockRdd.rdd(), Encoders.bean(Stock.class));

        processStockDataset(stockRddConvertedToDataset);

        stockRddConvertedToDataset
                .withColumn("date_str", functions.expr("cast(date as string)"))
                .show(10, false);

        spark.close();

    }

    public static void main(String ... args) throws Exception {
        new DataFrameDemo().start(args);
    }
}

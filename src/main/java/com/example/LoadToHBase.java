package com.example;

import com.example.helper.Stock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LoadToHBase implements Serializable {


    private SparkSession spark = null;
    private SparkConf conf = null;

    public LoadToHBase() {
        conf = new SparkConf()
                .setAppName(getClass().getName())
                .setIfMissing("spark.master", "local[*]")
                .setIfMissing("spark.driver.memory", "4g");
        spark = SparkSession.builder().config(conf).getOrCreate();
    }

    public Dataset<Row> loadCsv(String path) {
        Dataset<Row> dataset = spark.read().option("inferSchema", true).option("header", true).csv(path);
        return dataset;
    }

    private void saveStockRecords(Iterator<Stock> rows) {
        Configuration configuration = HBaseConfiguration.create();
        String path = LoadToHBase.class
                .getClassLoader()
                .getResource("hbase-site.xml")
                .getPath();
        configuration.addResource(new Path(path));

        Table table = null;
        Connection conn = null;
        try {
            conn = ConnectionFactory.createConnection(configuration);
            table = conn.getTable(TableName.valueOf("ns1:stocks"));
            List<Put> puts = new ArrayList<>();
            int batchSize = 2000;
            int count = 0;
            while (rows.hasNext()) {
                Stock stock = rows.next();
                puts.add(stock.toPut());
                if(puts.size() % batchSize == 0){
                    table.put(puts);
                    puts.clear();
                }
                ++count;
            }
            table.put(puts);
            System.out.println(String.format("Saving %d records", count));
            table.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            try {
                conn.close();
            }catch (IOException ex){
                ex.printStackTrace();
            }
        }
    }

    private Stock rowToStock(Row row){
        Stock stock = new Stock();

        stock.setDate(row.getAs("date"));

        stock.setOpen(row.getAs("open"));
        stock.setClose(row.getAs("close"));
        stock.setHigh(row.getAs("high"));
        stock.setLow(row.getAs("low"));
        stock.setClose(row.getAs("close"));
        stock.setAdjclose(row.getAs("adjclose"));
        stock.setVolume(row.getAs("volume"));
        stock.setSymbol(row.getAs("symbol"));

        return stock;
    }

    public void saveToHBase(String path) {
        Dataset<Row> dataset = loadCsv(path).withColumn("date"
                        , functions.expr("cast(`date` as date) as `date`"));

        Dataset<Stock> stockRows = dataset.map((MapFunction<Row, Stock>) row -> rowToStock(row), Encoders.bean(Stock.class));

        stockRows.show();

        stockRows.foreachPartition((ForeachPartitionFunction<Stock>) rows -> saveStockRecords(rows));
    }


    public void createHFiles(String path, String outputPath){
        Dataset<Row> dataset = loadCsv(path).withColumn("date"
                , functions.expr("cast(`date` as date) as `date`"));

        Dataset<Stock> stockRows = dataset.map((MapFunction<Row, Stock>) row -> rowToStock(row), Encoders.bean(Stock.class));

        stockRows.show();

        JavaPairRDD<ImmutableBytesWritable, Put> pairRdd = stockRows.javaRDD().mapToPair(r ->
                new Tuple2<>(r.toKey(), r.toPut()));


        Configuration configuration = HBaseConfiguration.create();
        String resourcePath = LoadToHBase.class
                .getClassLoader()
                .getResource("hbase-site.xml")
                .getPath();
        configuration.addResource(new Path(resourcePath));

        configuration.set(TableOutputFormat.OUTPUT_TABLE, "ns1:stocks");
        pairRdd.saveAsNewAPIHadoopFile(outputPath
                , ImmutableBytesWritable.class
                , Put.class
                , TableOutputFormat.class
                , configuration);

    }

    public Dataset<Row> sql(String statement){

        return spark.sql(statement);

    }

    public void close(){
        spark.close();
    }

    public static void main(String[] agrs) {
        String path = "/data/stocks.csv";
        LoadToHBase loadToHBase = new LoadToHBase();

        /*
        Dataset<Row> dataset = loadToHBase.loadCsv(path);

        dataset.show();

        dataset.filter("year(date) = 2016").groupBy("symbol").agg(functions.avg("volume")).show();

        dataset.createOrReplaceTempView("stocks");
        dataset.printSchema();
        loadToHBase.sql("select symbol, avg(volume) from stocks where year(date) = 2016 group by symbol").show();
        */
        loadToHBase.saveToHBase(path);
        //loadToHBase.createHFiles(path, "/tmp/stocks_hfile");
        loadToHBase.close();

    }
}

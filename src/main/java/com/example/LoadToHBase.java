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
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

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
                .setIfMissing("spark.master", "local[*]");
        spark = SparkSession.builder().config(conf).getOrCreate();
    }

    private Dataset<Row> loadCsv(String path) {
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
            while (rows.hasNext()) {
                Stock stock = rows.next();
                puts.add(stock.toPut());
            }
            System.out.println(String.format("Saving %d records", puts.size()));
            Object[] results = new Object[puts.size()];
            table.put(puts);
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

    public void saveToHBase(String path) {
        Dataset<Row> dataset = loadCsv(path).withColumn("date"
                        , functions.expr("cast(`date` as date) as `date`"));

        Dataset<Stock> stockRows = dataset.map((MapFunction<Row, Stock>) row -> {
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
        }, Encoders.bean(Stock.class));

        stockRows.show();

        stockRows.foreachPartition((ForeachPartitionFunction<Stock>) rows -> saveStockRecords(rows));

    }
    public void close(){
        spark.close();
    }

    public static void main(String[] agrs) {
        String path = "/data/stocks.csv";
        LoadToHBase loadToHBase = new LoadToHBase();
        loadToHBase.saveToHBase(path);
        loadToHBase.close();

    }
}

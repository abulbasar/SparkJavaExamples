package com.example.dataframe;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.sql.Timestamp;
import java.time.DayOfWeek;

import static org.apache.spark.sql.functions.expr;

public class RDD2DF extends SparkBaseApp{


    public Namespace parseArguments(String[] args) throws ArgumentParserException {
        final ArgumentParser parser = ArgumentParsers.newFor("prog").build()
                .description("My Spark application for movie lens dataset");

        parser.addArgument("-i", "--input-file")
                .help("Path for stocks file").required(true);
        Namespace res = parser.parseArgs(args);
        return res;
    }

    public void start(String[] args) throws ArgumentParserException {
        final Namespace namespace = parseArguments(args);
        final String inputFile = namespace.getString("input_file");

        initSparkSession();

        Dataset<Row> dataset = sparkSession.read().text(inputFile);
        dataset.show(10, false);
        //Convert dataset into rdd
        println("--------------------- Convert the dataset into rdd ----------------");
        final JavaRDD<Row> rowJavaRDD = dataset.javaRDD();
        rowJavaRDD.take(10).forEach(this::println);

        println("--------------------- Convert the dataset into string dataset ----------------");
        final Dataset<String> stringDataset = dataset.as(Encoders.STRING());
        stringDataset.show(10, false);

        show(stringDataset.javaRDD().map(String::toUpperCase));

        dataset = sparkSession.read()
                .option("header", true)
                .option("inferSchema", true)
                .option("samplingRatio", 0.001)
                .format("csv")
                .load(inputFile)
                ;

        // Convert generic Row class dataset to Stock class dataset
        final Dataset<Stock> stockDataset = dataset.map((MapFunction<Row, Stock>) (Row row) -> {
            final Stock stock = new Stock();
            stock.setDate(row.getTimestamp(0));
            stock.setOpen(row.getDouble(1));
            stock.setHigh(row.getDouble(2));
            stock.setLow(row.getDouble(3));
            stock.setClose(row.getDouble(4));
            stock.setVolume(row.getDouble(5));
            stock.setAdjclose(row.getDouble(6));
            stock.setSymbol(row.getString(7));
            return stock;
        }, Encoders.bean(Stock.class));
        stockDataset.show();


        analyzeStockDataWithGenericRow(dataset);
        analyzeStockDataWithStockClassDataset(stockDataset);


        println("---------------------- Filter function based on Stock class field ------------------------------");
        stockDataset
                .filter((Stock r) -> "INTC".equals(r.getSymbol())).show();

        println("---------------------- Mix java code and Dataset DSL (find all INTC records for Friday ------------------------------");
        stockDataset
                .filter((Stock r) -> {
                    if("INTC".equals(r.getSymbol())){
                        final Timestamp timestamp = r.getDate();
                        return timestamp.toLocalDateTime().getDayOfWeek().equals(DayOfWeek.FRIDAY);
                    }
                    return false;
                })
                .withColumn("DayOfWeek", expr("dayofweek(date)"))
                .show();


        // Convert the Stock class dataset to Stock class RDD
        final JavaRDD<Stock> stockJavaRDD = stockDataset.javaRDD();


        // Convert Stock class Rdd into Stock class Dataset
        final Dataset<Stock> rowDataset = sparkSession
                .createDataFrame(stockJavaRDD, Stock.class)
                .as(Encoders.bean(Stock.class))
                ;

    }

    // The argument is dataset of Row, hence we cannot guarantee that all necessary fields have
    // been passed in the dataset. So you code might throw run time exception when it cannot find
    // required columns
    public void analyzeStockDataWithGenericRow(Dataset<Row> dataset){

    }

    // This ensure that input is always Stock class dataset
    // Hence we can get compile time check about the input
    public void analyzeStockDataWithStockClassDataset(Dataset<Stock> dataset){

    }




    public static void main(String[] args) throws ArgumentParserException {
        final RDD2DF stockApp = new RDD2DF();
        stockApp.start(args);
        stockApp.close();
    }
}

package com.example.dataframe;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class HiveTableApp {

    public static void main(String[] args) throws Exception{
        final HiveTableApp app = new HiveTableApp();
        app.start(args);
    }

    private SparkSession getSparkSession(){
        final SparkConf sparkConf = new SparkConf()
                .setAppName(getClass().getName())
                .setIfMissing("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
                .setIfMissing("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
                .setIfMissing("spark.master", "local[*]");
        final SparkSession sparkSession = SparkSession
                .builder()
                .enableHiveSupport()
                .config(sparkConf)
                .getOrCreate();
        System.out.println("Spark UI: " + sparkSession.sparkContext().uiWebUrl());
        return sparkSession;
    }

    public Namespace parseArguments(String[] args) throws Exception {
        final ArgumentParser parser = ArgumentParsers
                .newFor("StocksApp").build()
                .description("Stocks application using java");

        parser.addArgument("-f", "--file")
                .help("Path for stocks file")
                .required(true);

        parser.addArgument("-t", "--table-name")
                .help("Target table name")
                .required(true);

        parser.addArgument("-d", "--database")
                .help("Target database name in hive")
                .required(false)
                .setDefault("default");

        Namespace res = parser.parseArgs(args);
        return res;
    }


    private void start(String[] args) throws Exception {

        final Namespace namespace = parseArguments(args);
        final String inputFilePath = namespace.getString("file");
        final String tableName = namespace.getString("table_name");
        final String database = namespace.getString("database");

        final SparkSession sparkSession = getSparkSession();

        final Dataset<Row> dataset = sparkSession.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("samplingRatio", "0.0001")
                .load(inputFilePath);

        dataset.show();

        final String currentDatabase = sparkSession.catalog().currentDatabase();
        System.out.println("Current database: "
                + currentDatabase);

        System.out.println("--- Show databases -------------");
        sparkSession.catalog().listDatabases().show();



        if(!currentDatabase.equals(database) && database != null){

            sparkSession.sql("create database if not exists " + database);

            sparkSession.catalog().setCurrentDatabase(database);
            System.out.println("Switched to " + database);



        }

        sparkSession.sql("show tables");


        System.out.println("Saving the dataset to hive");
        dataset.write().mode(SaveMode.Overwrite).saveAsTable(tableName);
        System.out.println("Saved the dataset to hive");

        System.out.println("------ Show existing tables -------------");
        sparkSession.catalog().listTables().show();

        System.out.println("---- Reloaded table, created dataset from the hive table name ---- ");
        sparkSession.table(tableName).show();




    }

}

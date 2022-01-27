package com.example.dataframe;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class FileTypesApp {

    private transient SparkSession session;

    public static void main(String[] args) throws Exception{
        final FileTypesApp app = new FileTypesApp();
        app.start(args);
        System.out.println("Application is complete");
        Thread.sleep(1000000000);
    }

    private Namespace parseArguments(String[] args) throws ArgumentParserException {
        ArgumentParser parser = ArgumentParsers.newFor("SFPDAnalysis")
                .build()
                .description("Application to analyze sfpd dataset");
        parser.addArgument("-i", "--input")
                .required(true)
                .help("Input file");

        parser.addArgument("-s", "--sample")
                .required(true)
                .help("Sample file for schema inferring");

        Namespace res = parser.parseArgs(args);
        return res;
    }


    private SparkSession getSparkSession(){
        final SparkConf sparkConf = new SparkConf()
                .setAppName(getClass().getName())
                .setIfMissing("spark.master", "local[*]");

        final SparkSession session = SparkSession.builder().config(sparkConf).getOrCreate();

        final SparkContext sparkContext = session.sparkContext(); // Scala spark context
        System.out.println("Spark WebUI: " + sparkContext.uiWebUrl());
        final JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);
        // This is required when you want to create RDD and convert the RDD to Dataframe
        return session;
    }


    private void start(String[] args) throws Exception{
        System.out.println("My DataFrame application");

        final Namespace namespace = parseArguments(args);
        final String inputPath = namespace.getString("input");
        final String sampleFile = namespace.getString("sample");
        System.out.println("Input path: " + inputPath);
        System.out.println("Sample file path: " + sampleFile);

        final SparkSession sparkSession = getSparkSession();


        final long startTime = System.currentTimeMillis();

        // https://spark.apache.org/docs/latest/sql-data-sources-json.html
        Dataset<Row> dataset = sparkSession.read()
                .option("samplingRatio", "0.0001")
                .json(sampleFile)
        ;

        final StructType schema = dataset.schema();


        dataset = sparkSession.read().schema(schema).json(inputPath);

        dataset.printSchema();

        final long recordCount = dataset.count();
        System.out.println("Number of records: " + recordCount);

        final long duration = (System.currentTimeMillis() - startTime)/1000;
        System.out.println("Duration (secs) for the load tweets: " + duration);

        /*
        Without sampling
        --------------------------------------
        Number of records: 1307465
        Duration (secs) for the load tweets: 47

        With samplingRatio 0.0001
        --------------------------------------
        Number of records: 1307465
        Duration (secs) for the load tweets: 22

        With samplingRatio 0.0001 and sample file
        --------------------------------------
        Number of records: 1307465
        Duration (secs) for the load tweets: 18

        */

        final Dataset<Row> tweets = dataset.selectExpr(
                "id",
                "text",
                "source",
                "truncated",
                "user.id as user_id",
                "user.screen_name",
                "user.verified",
                "lang",
                "timestamp_ms"
        );

        tweets.show();

        String pathPrefix = "/tmp/tweets/";

        System.out.println("Writing csv");
        tweets.write().mode(SaveMode.Overwrite).format("csv")
                .option("header", "true")
                .save(pathPrefix + "csv");

        System.out.println("Writing csv with gzip compression");
        tweets.write().mode(SaveMode.Overwrite).format("csv")
                .option("header", "true")
                .option("compression", GzipCodec.class.getName())
                .save(pathPrefix + "csv_gz");

        System.out.println("Writing json with no compression");
        tweets.write().mode(SaveMode.Overwrite).format("json")
                .save(pathPrefix + "json");

        System.out.println("Writing json with gzip compression");
        tweets.write().mode(SaveMode.Overwrite).format("json")
                .option("compression", GzipCodec.class.getName())
                .save(pathPrefix + "json_gz");

        System.out.println("Writing xml with no compression");
        tweets.write().mode(SaveMode.Overwrite).format("xml")
                .save(pathPrefix + "xml");

        System.out.println("Writing xml with gzip compression");
        tweets.write().mode(SaveMode.Overwrite).format("xml")
                .option("compression", GzipCodec.class.getName())
                .save(pathPrefix + "xml_gz");

        // Avro supported compression: uncompressed , snappy , and deflate
        System.out.println("Writing avro with no compression");
        tweets.write().mode(SaveMode.Overwrite).format("avro")
                .option("compression", "uncompressed")
                .save(pathPrefix + "avro");

        System.out.println("Writing avro with snappy compression");
        tweets.write().mode(SaveMode.Overwrite).format("avro")
                .option("compression", "snappy")
                .save(pathPrefix + "avro_snappy");

        System.out.println("Writing parquet with no compression");
        tweets.write().mode(SaveMode.Overwrite).format("parquet")
                .save(pathPrefix + "parquet");

        System.out.println("Writing parquet with gzip compression");
        tweets.write().mode(SaveMode.Overwrite).format("parquet")
                .option("compression", "gzip")
                .save(pathPrefix + "parquet_gz");

        System.out.println("Writing parquet with snappy compression");
        tweets.write().mode(SaveMode.Overwrite).format("parquet")
                .option("compression", "snappy")
                .save(pathPrefix + "parquet_snappy");

        //ORC supported compression codec:  brotli, uncompressed, lz4, gzip, lzo, snappy, none, zstd
        System.out.println("Writing orc with no compression");
        tweets.write().mode(SaveMode.Overwrite).format("orc")
                .save(pathPrefix + "orc");

//        System.out.println("Writing orc with gzip compression");
//        tweets.write().mode(SaveMode.Overwrite).format("orc")
//                .option("compression", "gzip")
//                .save(pathPrefix + "orc_gz");

        System.out.println("Writing orc with snappy compression");
        tweets.write().mode(SaveMode.Overwrite).format("orc")
                .option("compression", "snappy")
                .save(pathPrefix + "orc_snappy");



    }

}

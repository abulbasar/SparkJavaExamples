package com.example;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;



/*
*  Learning Goals:
*  1. Loading dataframe using spark packages
*  2. Caching dataframe
*  3. Understand the impact of storagelevel in dataframe caching
*  4. Defining UDF
*  5. Calling UDF from Dataframe DSL and Spark SQL
*  6. Saving data in various format - json, csv, parquet, orc and compare storage efficiency
*
* */

public class SFPDAnalysis {
    private static Logger logger = LoggerFactory.getLogger(SFPDAnalysis.class);

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: com.example.SFPDAnalysis <file>");
            System.exit(1);
        }

        logger.info("Input for SFPD dataset: " + args[0]);

        SparkConf sparkConf = new SparkConf().setAppName("SFPDAnalysis");
        sparkConf.setIfMissing("spark.master", "local[*]");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new HiveContext(sc);
        //SQLContext sqlContext = new SQLContext(sc);

        logger.info("Loading sfpd data into a dataframe");
        DataFrame sfpd = sqlContext
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(args[0]);

        sfpd.persist(StorageLevel.MEMORY_ONLY());

        /**
         * 	MEMORY_ONLY_SER 155.2 MB
         * 	MEMORY_ONLY 155.3 MB
         *
         * 	Conclusion: storage format in dataframe does not impact memory usage as it does in RDD
         *
         */

        sfpd.sample(false, 0.001, 100).show();

        logger.info("Saving dataframe to filesystem");
        sfpd
                .write()
                .format("orc")
                .mode(SaveMode.Overwrite)
                .save("output/sfpd");

        /*
        * Parquet: 47MB
        * ORC: 43 MB
        * JSON: 603MB
        * CSV: 364MB
        *
        * */


        logger.info("Total number of records: " + sfpd.count());

        sfpd
                .groupBy(sfpd.col("Category"))
                .count()
                .orderBy(functions.col("count").desc())
                .show(100);



        sqlContext.udf().register("myDayOfWeek", new UDF1<String, Integer>() {
            SimpleDateFormat format = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss aaa");
            Calendar cal = Calendar.getInstance();

            public Integer call(String value) throws Exception{
                Date date = format.parse(value);
                cal.setTime(date);
                return cal.get(Calendar.DAY_OF_WEEK);
            }

        }, DataTypes.IntegerType);

        logger.info("Calling UDF in dataframe DSL");
        sfpd.select("Date").show(10, false);

        sfpd
                .withColumn("MyDayOfWeek", functions.callUDF("myDayOfWeek", (sfpd.col("Date"))))
                .show();


        logger.info("Calling UDF in SQL statement");

        sfpd.registerTempTable("sfpd");
        String sqlText = "select *, myDayOfWeek(`Date`) MyDayOfWeek from sfpd";
        sqlContext.sql(sqlText).show();


        System.out.println("Process has finished. Press <enter> to exit.");
        System.in.read();

        sc.stop();
        sc.close();



    }

}

package com.example.df;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class UnnestDataFrame {

    private Dataset<Row> flatten(Dataset<Row> input){
        final Tuple2<String, String>[] dtypes = input.dtypes();
        Dataset<Row> df = input;
        for (Tuple2<String, String> dtype : dtypes) {
            final String name = dtype._1;
            final String type = dtype._2;
            if(type.startsWith("StructType")){
                df = df.selectExpr("*", name + ".*");
            }
        }
        return df;
    }


    public void start(String ... args) throws Exception{

        if (args.length != 1) {
            System.out.println("Provide input path ");
            System.exit(1);
        }

        final String inputPath = args[0];

        SparkConf conf = new SparkConf();
        conf.setAppName(StockAnalysis.class.getName());
        conf.setIfMissing("spark.master", "local[*]");


        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        final Dataset<Row> df = spark.read()
                .json(inputPath);

        df.show();

        flatten(df).show(10, false);

        df.printSchema();


        System.out.println("Count of records: " + df.count());

        //df.select("userIdentity.arn").as(Encoders.STRING()).show(10, false);

        spark.close();

    }

    public static void main(String ... args) throws Exception {
        new UnnestDataFrame().start(args);
    }
}

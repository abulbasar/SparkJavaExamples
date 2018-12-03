package com.example;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/*
* 1. Create a DStream based on socket stream source
* 2. Specify persistence level to MEMORY_ONLY
* 3. Save the raw stream to file system
* 4. Find frequency distribution of media using a window operation
* */

public class StreamApp {


    private static Logger logger = LoggerFactory.getLogger(StreamApp.class);



    public static void main(String[] args) throws Exception {

        final String rawOutput = "output/stream-raw";

        SparkConf sparkConf = new SparkConf().setAppName("StreamApp");
        sparkConf.setIfMissing("spark.master", "local[*]");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        final SQLContext sqlContext = new SQLContext(sc);

        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(5));

        Integer port = 9999;

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("localhost"
                , port, StorageLevel.MEMORY_ONLY());
        lines.print(10);

        //Saving the raw DStream to file system
        lines.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
            public void call(JavaRDD<String> stringJavaRDD, Time time) throws Exception {
                stringJavaRDD.coalesce(1).saveAsTextFile(rawOutput + "/"
                        + time.toString().split(" ")[0]);
            }
        });
        
        

        sc.getConf().set("spark.sql.shuffle.partitions", "1");

        sqlContext.udf().register("findMedia", new UDF1<String, String>() {

            public String call(String value){

                String media = null;

                if(value.startsWith("http://twitter.com/download/")){
                    String[] tokens = value.split("/");
                    media = tokens[tokens.length - 1];
                }else if(value.startsWith("https://mobile.twitter.com")){
                    media = "twitter-mobile";
                }else if(value.startsWith("http://twitter.com")
                        || value.startsWith("http://www.twitter.com")){
                    media = "twitter.com";
                }else{
                    media = "other";
                }
                return media;
            }

        }, DataTypes.StringType);


        lines
            .window(Durations.seconds(30), Durations.seconds(10))
            .foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
                public void call(JavaRDD<String> stringJavaRDD, Time time)
                        throws Exception {
                    logger.info("No of records: " + stringJavaRDD.count());
                    if(!stringJavaRDD.isEmpty()) {
                        Dataset<Row> df = sqlContext
                                .read()
                                .json(stringJavaRDD)
                                .filter(functions.col("id").isNotNull())
                                .select(functions.regexp_extract(functions.col("source"), "href=\\\"(.*?)\\\"", 1).alias("href"))
                                .withColumn("href", functions.lower(functions.col("href")))
                                .withColumn("media", functions.callUDF("findMedia", functions.col("href")))
                                .groupBy("media")
                                .count();
                        df.show(100, false);
                    }
                }
            });

        ssc.start();
        ssc.awaitTermination();

        ssc.stop();
        ssc.close();
    }


}

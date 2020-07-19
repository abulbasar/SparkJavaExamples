package com.example;

import com.example.models.Stock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class QueryHBaseTable {

    private SparkSession spark = null;
    private SparkConf conf = null;

    public QueryHBaseTable(){
        conf = new SparkConf()
                .setAppName(getClass().getName())
                .setIfMissing("spark.master", "local[*]")
                .setIfMissing("spark.driver.memory", "4g");
        spark = SparkSession.builder().config(conf).getOrCreate();
    }

    public void loadFromHBase() {

        Configuration configuration = HBaseConfiguration.create();
        String path = LoadToHBase.class
                .getClassLoader()
                .getResource("hbase-site.xml")
                .getPath();
        configuration.addResource(new Path(path));

        configuration.set(TableInputFormat.INPUT_TABLE, "ns1:stocks");


        JavaPairRDD<ImmutableBytesWritable, Result> rows = spark.sparkContext().newAPIHadoopRDD(configuration
                                    , TableInputFormat.class
                                    , ImmutableBytesWritable.class
                                    , Result.class).toJavaRDD().mapToPair(r -> r);

        Dataset<Row> df = spark.createDataFrame(rows.map(r -> Stock.parse(r._2)), Stock.class);
        df.cache();

        df.show();
        try {
            df.createTempView("stocks_hbase");
        }catch (Exception e){
            e.printStackTrace();
        }

        spark.sql("select symbol, max(100*(close-open)/open) as pct_max from stocks_hbase where year(date) = 2016 group by symbol order by abs(pct_max) desc").show();



    }

    public static void main(String[] args){
        new QueryHBaseTable().loadFromHBase();
    }
}

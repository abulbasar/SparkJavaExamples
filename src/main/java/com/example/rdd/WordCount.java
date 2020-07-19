package com.example.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class WordCount {

    WordCount(){

    }

    private void show(JavaRDD<?> rdd){
        System.out.println("Number of partitions: " + rdd.partitions().size());
        final List<?> objects = rdd.take(10);
        for(int i=0;i<objects.size();++i){
            System.out.println(String.format("[%d]", i) + objects.get(i));
        }
    }


    public void start(){
        SparkConf conf = new SparkConf();
        conf.setAppName(getClass().getName());
        conf.setMaster("local[4]"); // 4 executor threads
        //conf.set("spark.master", "local[4]");
        //conf.set("spark.master", "yarn");
        conf.setIfMissing("spark.master", "local[4]");
        conf.setIfMissing("spark.default.parallelism", "16");

        JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<String> rdd = sc.textFile("/Users/abasar/data/story.txt", 6);
        System.out.println(rdd.count());

        rdd.takeSample(false, 1000000);
        final JavaRDD<String> repartitionedRdd = rdd.repartition(10);
        show(repartitionedRdd);

        final List<String>[] partitions = repartitionedRdd
                .collectPartitions(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});

        for (List<String> partition : partitions) {
            System.out.println("Partition size: " + partition.size());
        }


    }

    public static void main(String ... args){
        new WordCount().start();
    }
}

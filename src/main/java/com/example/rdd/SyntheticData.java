package com.example.rdd;

import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class SyntheticData extends SparkBaseApp{

    public void start(){
        initSparkSession();
        List<Double> values = new ArrayList<>();
        // Generate 100 random numbers and append them to values
        new Random().doubles(100).forEach(values::add);

        // Creating rdd from list
        final JavaRDD<Double> doubleJavaRDD = sc.parallelize(values);

        // creating list from rdd
        final List<Double> collect = doubleJavaRDD.collect();
        collect.forEach(System.out::println);


        // Creating an RDD of various data types. It is allowed in RDD
        final JavaRDD<? extends Serializable> rdd = sc.parallelize(Arrays.asList(1, 100L, "Hello", true, 3.14));

        rdd.collect().forEach(System.out::println);

    }

    public static void main(String[] args){
        new SyntheticData().start();
    }

}

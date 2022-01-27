package com.example.dataframe;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;

public class UnionApp {

    public static void main(String[] args) throws Exception{
        final UnionApp app = new UnionApp();
        app.start(args);
    }

    private SparkSession getSparkSession(){
        final SparkConf sparkConf = new SparkConf()
                .setAppName(getClass().getName())
                .setIfMissing("spark.master", "local[*]");
        final SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        //final JavaSparkContext javaSparkContext = new JavaSparkContext(sparkSession.sparkContext());
        System.out.println("Spark UI: " + sparkSession.sparkContext().uiWebUrl());
        return sparkSession;
    }


    @Data
    @AllArgsConstructor
    public static class Person implements Serializable {
        private String name;
        private String email;
        private Integer age;
        private Boolean verified;
    }

    @Data
    @AllArgsConstructor
    public static class Customer implements Serializable {
        private String firstName;
        private String email;
        private Double revenue;
    }




    private void start(String[] args) throws Exception {

        final SparkSession sparkSession = getSparkSession();


        System.out.println("---------------- Dataframe from persons list ---------------");
        Dataset<Row> personDf = sparkSession.createDataFrame(Arrays.asList(
                new Person("User 1", "user1@gmail.com", 30, false),
                new Person("User 2", "user2@gmail.com", 40, false),
                new Person("User 3", "user3@gmail.com", 50, false)
        ), Person.class);

        personDf.show();


        final Dataset<Row> customers = sparkSession.createDataFrame(Arrays.asList(
                new Customer("First Name 1", "user1@gmail.com", 3000.0),
                new Customer("First Name 1", "user1@gmail.com", 3000.0),
                new Customer("First Name 1", "user1@gmail.com", 3000.0),
                new Customer("First Name 1", "user1@gmail.com", 3000.0)
        ), Customer.class);


        personDf
                .selectExpr("name", "email", "age", "verified", "null")
                .union(
                        customers
                                .selectExpr("firstName", "email", "null","null", "revenue")
                ).show(30);


    }

}

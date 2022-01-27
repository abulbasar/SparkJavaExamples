package com.example.dataframe;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class SyntheticDataApp {

    public static void main(String[] args) throws Exception{
        final SyntheticDataApp app = new SyntheticDataApp();
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


    private void start(String[] args) throws Exception {

        final SparkSession sparkSession = getSparkSession();

        Dataset<Long> dataset = sparkSession.range(1000, 2000);
        final Dataset<Row> rowDataset = dataset.withColumn("v1", functions.randn());

        rowDataset.show();

        List<String> values = Arrays.asList("abul@gmail.com", null, "", "email@example.com");
        final Dataset<String> stringDataset = sparkSession.createDataset(values, Encoders.STRING());
        stringDataset.show();

        stringDataset.printSchema();


        final List<Row> rows = Arrays.asList(
                RowFactory.create("User 1", "user1@gmail.com", 30, false),
                RowFactory.create("User 2", "user2@gmail.com", 40, false),
                RowFactory.create("User 3", "user3@gmail.com", 50, false)
        );

        final StructType schema = new StructType()
                .add("name", DataTypes.StringType)
                .add("email", DataTypes.StringType)
                .add("age", DataTypes.IntegerType)
                .add("verified", DataTypes.BooleanType);

        sparkSession.createDataFrame(rows, schema).show();


        System.out.println("---------------- Dataframe from persons list ---------------");
        Dataset<Row> personDf = sparkSession.createDataFrame(Arrays.asList(
                new Person("User 1", "user1@gmail.com", 30, false),
                new Person("User 2", "user2@gmail.com", 40, false),
                new Person("User 3", "user3@gmail.com", 50, false)
        ), Person.class);

        personDf.show();

        System.out.println("---------------- Dataframe from persons rdd ---------------");

        final JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());

        final JavaRDD<Person> personsRdd = jsc.parallelize(Arrays.asList(
                new Person("User 1", "user1@gmail.com", 30, false),
                new Person("User 2", "user2@msn.com", 40, false),
                new Person("User 3", "user3@yahoo.com", 50, false),
                new Person("User 4", null, 50, false),
                new Person("User 5", "", 50, false)
        ));

        personDf = sparkSession.createDataFrame(personsRdd, Person.class);
        personDf.show();





    }

}

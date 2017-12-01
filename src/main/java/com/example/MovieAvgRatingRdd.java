package com.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

/*
* Steps :
1. Load movies.csv as an RDD
2. Transform the movies RDD to a Pair RDD with movieId as key
3. Load ratings.csv as an RDD
4. Transform the ratings RDD to a Pair RDD with movieId as key
5. Join the movies paired RDD and ratings Paired RDD
6. Group the newly created RDD by movieId
7. Find average rating for each movie
8. Create a new RDD with movieId, title, avg rating
9. Write the new RDD to the file system

*
* */

final public class MovieAvgRatingRdd {

	private static Logger logger = LoggerFactory.getLogger(MovieAvgRatingRdd.class);

	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			System.err.println("Usage: com.example.MovieAvgRatingRdd <movie file> <rating file> <Output path>");
			System.exit(1);
		}

		logger.info("Input for movies data set: " + args[0]);

		SparkConf sparkConf = new SparkConf().setAppName("Movie Lens Analyzer");
		sparkConf.setIfMissing("spark.master", "local[*]");

		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> moviesRdd = sc.textFile(args[0], 1);

		logger.info("Number of records in movies dataset: " + moviesRdd.count());

		logger.info("Sample values from the movies dataset");
		for (String s : moviesRdd.take(10)) {
			logger.info(s);
		}

		JavaPairRDD<String, String> moviesData = moviesRdd.filter(s -> !s.startsWith("movieId,title,genres"))
				.mapToPair(s -> {
					String[] tokens = s.split(",");
					return new Tuple2<>(tokens[0], tokens[1]);
				});

		logger.info("Loading ratings rdd");
		JavaRDD<String> ratingsRdd = sc.textFile(args[1], 1);

		JavaPairRDD<String, Double> ratingsData = ratingsRdd
				.filter(s -> !s.startsWith("userId,movieId,rating,timestamp")).mapToPair(s -> {
					String[] tokens = s.split(",");
					return new Tuple2<>(tokens[1], Double.valueOf(tokens[2]));
				});

		logger.info("No of records in ratings rdd: " + ratingsData.count());

		JavaPairRDD<String, Tuple2<String, Double>> joined = moviesData.join(ratingsData);
		// The values will movieId, title, and rating

		JavaPairRDD<String, Iterable<Tuple2<String, Double>>> grouped = joined.groupByKey();

		JavaPairRDD<String, Tuple2<String, Double>> avgRating = grouped.mapValues(values -> {

			Double sum = 0.0;
			Integer count = 0;
			String title = null;
			for (Tuple2<String, Double> t : values) {
				sum += t._2;
				++count;
				title = t._1;
			}
			Double avg = sum / count;
			return new Tuple2<>(title, avg);
		});

		JavaRDD<String> avgRatingFlat = avgRating.map(t -> t._1 + "," + t._2._1 + ", " + t._2._2);

		// Display the values
		for (String s : avgRatingFlat.collect()) {
			System.out.println(s);
		}

		logger.info("Saving the result to " + args[2]);
		avgRatingFlat.saveAsTextFile(args[2]);

		System.out.println("Process is complete.");

		// A better solution would have been find the average rating from ratings.csv
		// then join that result back with the movies.csv data

		sc.stop();
		sc.close();

	}

}

package com.example;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;



/*
* Leaning objectives:
* 1. Convert RDD into Dataframe using java bean
* 2. Use mapPartition function rather than map function for efficient processing
* 3. Use CSV parser to handle advanced handling of CSV data
* 4. Join operation on two Dataframe using Spark SQL
* 5. Join operation of two dataframe using Dataframe DSL
* 6. Sample records from dataframe
* 7. Reduce partition to control number of part files while saving the dataframe
* 8. Use save mode and data format while saving the data frame to filesystem
*
* */

final public class MovieLens {

    private static Logger logger = LoggerFactory.getLogger(MovieLens.class);

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: com.example.MovieLens <movie file> <rating file>");
            System.exit(1);
        }

        logger.info("Input for movies data set: " + args[0]);

        SparkConf sparkConf = new SparkConf().setAppName("Movie Lens Analyzer");
        sparkConf.setIfMissing("spark.master", "local[*]");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);


        JavaRDD<String> moviesRdd = sc.textFile(args[0], 1);

        logger.info("Number of records in movies dataset: " + moviesRdd.count());

        logger.info("Sample values from the movies dataset");
        for (String s : moviesRdd.take(10)) {
            logger.info(s);
        }

        JavaRDD<String> moviesData = moviesRdd.filter(s -> !s.startsWith("movieId,title,genres"));
        
        


        JavaRDD<Movie> moviesObjects = moviesData.mapPartitions(input -> {
            CsvParserSettings parserSettings = new CsvParserSettings();
            parserSettings.setLineSeparatorDetectionEnabled(true);
            parserSettings.getFormat().setDelimiter(',');
            parserSettings.getFormat().setQuote('"');

            CsvParser parser = new CsvParser(parserSettings);
            ArrayList<Movie> movies = new ArrayList<Movie>();
        

            while (input.hasNext()) {
                String line = input.next();
                String[] tokens = parser.parseLine(line);
                Movie m = new Movie();
                m.setMovieId(Integer.valueOf(tokens[0]));
                m.setName(tokens[1]);
                m.setGenres(tokens[2]);
                movies.add(m);
            }
            return movies.iterator();
        });

        logger.info("Sample values from moviesObjects RDD ( ... RDD of class Movie");

        for (Movie m : moviesObjects.take(10)) {
            logger.info(m.toString());
        }

        Dataset<Row> moviesDf = sqlContext.createDataFrame(moviesObjects, Movie.class);

        logger.info("Sample values from moviesDf");
        moviesDf
                .sample(false, 0.001, 100)
                .show(100, false);
        moviesDf.printSchema();


        moviesDf.registerTempTable("movies");

        JavaRDD<String> ratingsRdd = sc.textFile(args[1], 1);

        logger.info("Number of records in ratings dataset: " + ratingsRdd.count());

        logger.info("Sample values from the ratingsRdd dataset");
        for (String s : ratingsRdd.take(10)) {
            logger.info(s);
        }

        JavaRDD<String> ratingsData = ratingsRdd.filter(s -> !s.startsWith("userId,movieId,rating,timestamp"));

        JavaRDD<MovieRating> ratingsObjects = ratingsData.mapPartitions(input -> {
            CsvParserSettings parserSettings = new CsvParserSettings();
            parserSettings.setLineSeparatorDetectionEnabled(true);
            parserSettings.getFormat().setDelimiter(',');
            parserSettings.getFormat().setQuote('"');

            final CsvParser parser = new CsvParser(parserSettings);
            ArrayList<MovieRating> ratings = new ArrayList<MovieRating>();

            while (input.hasNext()) {
                String line = input.next();
                String[] tokens = parser.parseLine(line);
                MovieRating r = new MovieRating();

                r.setUserId(Integer.valueOf(tokens[0]));
                r.setMovieId(Integer.valueOf(tokens[1]));
                r.setRating(Float.valueOf(tokens[2]));

                ratings.add(r);
            }
            return ratings.iterator();
        });

        Dataset<Row> ratingsDf = sqlContext.createDataFrame(ratingsObjects, MovieRating.class);
        ratingsDf.printSchema();

        logger.info("Show sample values from rating data set");
        ratingsDf.sample(false, 0.001, 100).show(100, false);

        ratingsDf.registerTempTable("ratings");

        String sqlText = "select t1.name, t1.movieId, avg(t2.rating) avg_rating from movies t1 left join " +
                " ratings t2 on t1.movieId = t2.movieId group by t1.movieId, t1.name";

        Dataset<Row> avgMovieRating = sqlContext.sql(sqlText);

        avgMovieRating.show(20, false);

        logger.info("Saving avgMovieRating dataframe to filesystem");

        avgMovieRating
                .coalesce(1) // Reduce the number of partition to 1 so that it produces a single file
                .write()
                .mode(SaveMode.Overwrite) // Overwrite any existing files in the target directory
                .format("json") // Output format will be json
                .save("output/AvgMovieRating");


        logger.info("Calculating avg movie rating using dataframe DSL");
        Dataset<Row> avgRatingDSL = ratingsDf
                .join(moviesDf, ratingsDf.col("movieId").equalTo(moviesDf.col("movieId")))
                .groupBy(ratingsDf.col("movieId"), moviesDf.col("name"))
                .agg(functions.avg(ratingsDf.col("rating")));

        avgRatingDSL.show(false);

        System.out.println("Process is complete.");

        sc.stop();
        sc.close();

    }

}

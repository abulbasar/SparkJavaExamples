package com.example.rdd;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import lombok.Data;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Closeable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MovieLens implements Closeable {

    /*
    * Download the data files from https://drive.google.com/open?id=16etgjQXq4LAtbEn7Rs4DBDWmanxfUiSf

    Example of command line argument:
    --ratings-file /home/abasar/data/ml-latest-small/ratings.csv --movies-file /home/abasar/data/ml-latest-small/movies.csv
    * */

    transient SparkConf sparkConf;
    transient JavaSparkContext sparkContext;

    public static CsvParser getCsvParser(){
        CsvParserSettings settings = new CsvParserSettings();
        //the file used in the example uses '\n' as the line separator sequence.
        //the line separator sequence is defined here to ensure systems such as MacOS and Windows
        //are able to process this file correctly (MacOS uses '\r'; and Windows uses '\r\n').
        settings.getFormat().setLineSeparator("\n");

        // creates a CSV parser
        CsvParser parser = new CsvParser(settings);
        return parser;
    }

    @Data
    public static class Movie implements Serializable {
        private Integer movieId;
        private String title;
        private String genres;

        public Movie(String[] tokens){
            movieId = Integer.parseInt(tokens[0]);
            title = tokens[1];
            genres = tokens[2];
        }
    }

    @Data
    public static class Rating implements Serializable {
        private Integer movieId;
        private Integer userId;
        private Double rating;
        private String title;
        private Long ts;

        public static Double toDouble(String s){
            return Double.parseDouble(s);
        }

        public Rating(String[] tokens){
            userId = Integer.parseInt(tokens[0]);
            movieId = Integer.parseInt(tokens[1]);
            rating = toDouble(tokens[2]);
            ts = Long.valueOf(tokens[3]);
        }
    }

    public void init(){
        sparkConf = new SparkConf()
                .setAppName(getClass().getName())
                .setIfMissing("spark.master", "local[*]");
        sparkContext = new JavaSparkContext(sparkConf);
    }

    public Namespace parseArguments(String[] args) throws Exception {
        final ArgumentParser parser = ArgumentParsers
                .newFor("MoviesApp").build()
                .description("Movies application using java");

        parser.addArgument("-m", "--movies-file")
                .help("Path for movies file")
                .required(true);

        parser.addArgument("-r", "--ratings-file")
                .help("Path for ratings file")
                .required(true);

        Namespace res = parser.parseArgs(args);
        return res;
    }

    static void println(Object object){
        System.out.println(object);
    }

    public static void show(JavaRDD<?> rdd){
        show(rdd, 10);
    }
    public static void show(JavaRDD<?> rdd, int count){
        println("Number of records: " + rdd.count());
        final List<?> objects = rdd.take(count); // rdd action
        for (Object object : objects) {
            println(object);
        }
    }

    @Data
    public static class MovieStats implements Serializable{
        private Integer movieId;
        private String title;
        private Double avgRating;
        private Integer countOfRating;
    }

    private void start(String[] args) throws Exception {
        final Namespace namespace = parseArguments(args);
        final String moviesFiles = namespace.getString("movies_file");
        final String ratingsFile = namespace.getString("ratings_file");

        init();

        // Create RDD
        final JavaRDD<String> moviesRawRdd = sparkContext.textFile(moviesFiles);
        println("MoviesRawRDD partitions: " + moviesRawRdd.getNumPartitions());

        final JavaRDD<String> ratingsRawRdd = sparkContext.textFile(ratingsFile);

        // filter out the data which begins with the header
        final JavaRDD<Movie> moviesDataRdd = moviesRawRdd
                .filter(line -> !line.startsWith("movieId")) // Filter is a transformation
                .mapPartitions((Iterator<String> batch) -> {
                    List<Movie> movies = new ArrayList<>();
                    final CsvParser csvParser = getCsvParser();
                    while (batch.hasNext()){
                        final String line = batch.next();
                        final String[] tokens = csvParser.parseLine(line);
                        final Movie movie = new Movie(tokens);
                        movies.add(movie);
                    }
                    return movies.iterator();
                })
                ;

        final JavaRDD<Rating> ratingsDataRdd = ratingsRawRdd
                .filter(line -> !line.startsWith("userId")) // Filter is a transformation
                .mapPartitions((Iterator<String> batch) -> {
                    List<Rating> ratings = new ArrayList<>();
                    final CsvParser csvParser = getCsvParser();
                    while (batch.hasNext()){
                        final String line = batch.next();
                        final String[] tokens = csvParser.parseLine(line);
                        final Rating rating = new Rating(tokens);
                        ratings.add(rating);
                    }
                    return ratings.iterator();
                })
                .cache()
                ;


        println("MoviesDataRdd partitions: " + moviesRawRdd.getNumPartitions());
        // count is action
        println("Number of records in movies rdd excluding the header: " + moviesDataRdd.count());
        println("Number of records in ratings rdd excluding the header: " +ratingsDataRdd.count());

        final List<Integer> partitionSize = moviesRawRdd.glom().map(List::size).collect();
        println("Number of records per partitions: ");
        for (Integer size : partitionSize) {
            println(size);
        }

        println("-----------The movies for which title contains Godfather----------------");
        final JavaRDD<Movie> godfatherRdd = moviesDataRdd.filter(movie -> movie.getTitle().contains("Godfather"));
        show(godfatherRdd);

        println("----------- MovieId for \"Godfather, The (1972)\"----------------");
        final Integer movieIdForGodfather1972 = moviesDataRdd
                .filter(movie -> movie.getTitle().equals("Godfather, The (1972)"))
                .map(Movie::getMovieId)
                .collect()
                .get(0);

        println("----------- Find avg rating and rating count per movie Id using ratings ----------------");

        final JavaRDD<MovieStats> movieStats = ratingsDataRdd
                .keyBy(Rating::getMovieId)
                .groupByKey()
                .mapValues((Iterable<Rating> batch) -> {
                    double sum = 0.0;
                    int count = 0;
                    Integer movieId = null;
                    for (Rating rating : batch) {
                        movieId = rating.movieId;
                        sum += rating.getRating();
                        count++;
                    }
                    final double avg = sum / count;
                    final MovieStats stats = new MovieStats();
                    stats.setMovieId(movieId);
                    stats.setAvgRating(avg);
                    stats.setCountOfRating(count);
                    return stats;
                })
                .map(Tuple2::_2);

        show(movieStats);

        println("----------- Avg rating and rating count for Godfather 1972 ----------------");

        show(movieStats.filter(stats -> stats.getMovieId().equals(movieIdForGodfather1972)));

        println("----------------- Find movies that have received more than 100 ratings -------------------------");

        println(movieStats.filter(stat -> stat.getCountOfRating()>100).count());

        println("------------------- Populate title in movieStats by joining movieStats Rdd with Movies Rdd--------------------------");
        final JavaRDD<MovieStats> movieStatsWithTitle = movieStats
                .keyBy(MovieStats::getMovieId)
                .join(moviesDataRdd.keyBy(Movie::getMovieId))
                .map(p -> {
                    final Tuple2<MovieStats, Movie> tuple = p._2();
                    final MovieStats stats = tuple._1();
                    final Movie movie = tuple._2();
                    stats.setTitle(movie.getTitle());
                    return stats;
                });

        show(movieStatsWithTitle);

        println("----------------- Find top 10 movies by highest avg ratings that have received more than 100 ratings -------------------------");

        final Iterator<MovieStats> top10MovieStatsIterator = movieStatsWithTitle
                .filter(stat -> stat.getCountOfRating() > 100)
                .sortBy(MovieStats::getAvgRating, false, 1)
                .toLocalIterator();

        List<MovieStats> top10Movies = new ArrayList<>();
        while (top10MovieStatsIterator.hasNext() && top10Movies.size()<=10){
            top10Movies.add(top10MovieStatsIterator.next());
        }
        for (MovieStats top10Movie : top10Movies) {
            println(top10Movie);
        }

        println("------------------ Save movieStatsWithTitle to file ----------------------");
        movieStatsWithTitle
                .filter(stat -> stat.getCountOfRating()>100)
                .sortBy(MovieStats::getAvgRating, false, 1)
                .saveAsTextFile("/tmp/movieStatsWithTitle");

    }

    public static void main(String[] args) throws Exception {
        System.out.println("Spark application");
        final MovieLens app = new MovieLens();
        app.start(args);
        app.close();
        System.out.println("Processing completed");
    }

    public void close() {
        sparkContext.close();
    }


}

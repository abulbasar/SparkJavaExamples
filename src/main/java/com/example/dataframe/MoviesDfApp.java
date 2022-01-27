package com.example.dataframe;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;

import java.io.Closeable;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.example.common.Utils.println;

public class MoviesDfApp implements Closeable {
    private SparkConf sparkConf;
    private SparkSession sparkSession;


    public static void main(String[] args) throws Exception {
        final MoviesDfApp app = new MoviesDfApp();
        app.start(args);
        app.close();
    }

    private Dataset<Row> createDataset(String path){
        final DataFrameReader dataFrameReader = sparkSession.read();
        final Dataset<Row> dataset = dataFrameReader
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")// spark tries to infer the type of column based on the values
                .option("samplingRatio", 0.001)
                .load(path)
                .cache()
                ;
        return dataset;
    }

    private void start(String[] args) throws Exception {
        final Namespace namespace = parseArguments(args);
        final String moviesFile = namespace.getString("movies_file");
        final String ratingsFile = namespace.getString("ratings_file");
        println("moviesFile: " + moviesFile);
        println("ratingsFile: " + ratingsFile);
        init();
        final Dataset<Row> movies = createDataset(moviesFile);
        println("------------------ Peek at movies dataframe-----------------------------");
        movies.show(10, false);
        println("------------------ Schema of movies dataframe-----------------------------");
        movies.printSchema();

        println("Number of records in movies dataframe: " + movies.count());

        final Dataset<Row> ratings = createDataset(ratingsFile);
        println("Number of records in ratings dataframe: " + ratings.count());
        println("------------------ Peek at rating dataframe-----------------------------");
        ratings.show();

        println("------------ Existing views before registering views for movies and rating ----------");
        sparkSession.sql("show tables").show();

        movies.createOrReplaceTempView("movies");
        ratings.createOrReplaceTempView("ratings");

        println("------------ Existing views after registering views for movies and rating ----------");
        sparkSession.sql("show tables").show();

        println("Spark current database: " + sparkSession.catalog().currentDatabase());
        sparkSession.catalog().listTables().show();

        println("-------------- Using Spark SQL, find the top 10 movies ---------------------------");
        sparkSession.sql("select " +
                "t1.movieId, t1.title, avg(t2.rating) avg_rating, " +
                "count(t1.movieId) as rating_count " +
                "from movies t1 join ratings t2 " +
                "on t1.movieId = t2.movieId " +
                "group by t1.movieId, t1.title " +
                "having count(t1.movieId)>100 " +
                "order by avg_rating desc limit 10")
                .show(10, false);

        //////////////////// DATAFRAME DSL //////////////////////////////////////

        println("----------------- Top 10 movies using spark dataframe api----------------");
        final Dataset<Row> top10Movies = movies.alias("t1")
                .join(ratings.alias("t2"),
                        functions.col("t1.movieId").equalTo(functions.col("t2.movieId")))
                .groupBy("t1.movieId", "t1.title")
                .agg(functions.avg("t2.rating").alias("avg_rating")
                        , functions.count("t2.rating").alias("rating_count")
                )
                .filter("rating_count>100")
                .orderBy(functions.desc("avg_rating"))
                .limit(10)
                .select("t1.movieId", "t1.title", "avg_rating", "rating_count")
                ;
        top10Movies.show(10, false);

        println("----------------- Top 3 best movies for each genre ----------------");

        final Dataset<Row> moviesWithGenre = movies
                .withColumn("genre", functions.expr("split(genres, '\\\\|')"))
                .withColumn("genre", functions.explode(functions.col("genre")));

        moviesWithGenre.show(10, false);
        moviesWithGenre.printSchema();

        final Dataset<Row> avgRatingDf = ratings
                .groupBy("movieId")
                .agg(functions.avg("rating").alias("avg_rating"),
                        functions.count("rating").alias("rating_count")
                )
                .filter("rating_count > 100");

        moviesWithGenre
                .join(avgRatingDf, "movieId")
                .show();

        final WindowSpec windowSpec = Window
                .partitionBy("genre")
                .orderBy(functions.desc("avg_rating"));

        final Dataset<Row> top3MoviesByEachGenre = moviesWithGenre
                .join(avgRatingDf, "movieId")
                .withColumn("rank", functions.rank().over(windowSpec))
                .filter("rank<=3")
                .orderBy(functions.asc("genre"), functions.asc("rank"))
                .drop("genres");
        top3MoviesByEachGenre.show(20, false);

        println("Number of partitions of top3MoviesByEachGenre: " + top3MoviesByEachGenre.rdd().getNumPartitions());

        println("---------------- Save the top3MoviesByEachGenre to /tmp/top3MoviesByEachGenre files ");

        top3MoviesByEachGenre
                .coalesce(1)// Coalescing will reduce the number of output part files to 1
                .write()
                .mode(SaveMode.Overwrite)
                .format("csv")
                .option("header", "true")
                .save("/tmp/Top3MoviesByGenre");

        println("----------------------- Create an UDF to extract the year from title -----------------------------------------");
        sparkSession.udf().register("extract_year_from_title", (String s)-> {
            final Pattern pattern = Pattern.compile(".*\\((\\d{4})\\).*");
            final Matcher matcher = pattern.matcher(s);
            if(matcher.find()){
                final String group = matcher.group(1);
                return Integer.parseInt(group);
            }
            return null;
        }, DataTypes.IntegerType);


        final Dataset<Row> moviesWithYear = movies
                .withColumn("year", functions.expr("extract_year_from_title(title)"))
                .sample(0.01);

        moviesWithYear.show(10, false);

        sparkSession
                .sql("select *, extract_year_from_title(title) year from movies")
                .sample(0.01)
                .show(10, false);

        println("-------------------------- Year with the highest number of movie realse ----------------");
        moviesWithYear
                .groupBy("year")
                .count()
                .orderBy(functions.desc("count"))
                .show();

    }

    private void init() {
        final SparkConf sparkConf = new SparkConf()
                .setAppName(getClass().getName())
                .setIfMissing("spark.master", "local[*]");

        sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

    }

    @Override
    public void close() throws IOException {
        sparkSession.close();
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
}

package com.example.dataframe;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class TweetAnalysisApp extends SparkBaseApp{

    /*


    */

    public Namespace parseArguments(String[] args) throws ArgumentParserException {
        final ArgumentParser parser = ArgumentParsers.newFor("prog").build()
                .description("Spark application for file format");

        parser.addArgument("-s", "--sample")
                .help("Sample file for schema inferencing")
                .required(false);

        parser.addArgument("-p", "--path")
                .help("Input path for files").required(true);

        Namespace res = parser.parseArgs(args);
        return res;
    }

    public void start(String[] args) throws ArgumentParserException {
        final Namespace namespace = parseArguments(args);
        final String sampleFile = namespace.getString("sample");
        final String inputPath = namespace.getString("path");

        initSparkSession();

        Dataset<Row> raw = sparkSession
                .read()
                .option("samplingRatio", "0.001")
                .json(sampleFile);
        //raw.show();

        //raw.printSchema();

        final StructType schema = raw.schema();


        raw = sparkSession.read().schema(schema).json(inputPath);

        final Dataset<Row> parsed = raw
                .selectExpr("id", "text", "source", "user.id as user_id", "user.verified",
                        "user.name", "user.screen_name", "lang", "timestamp_ms")
                .cache()
                ;
        parsed.show();



        final long totalCount = parsed.count();
        println("Total number of tweets: " + totalCount);

        final Dataset<Row> tweetsVerified = parsed.filter("user.verified = true");
        println("Number of tweets from verified accounts: " + tweetsVerified.count());

        final Double avgLength = parsed.agg(expr("avg(cast(length(text) as double))"))
                .as(Encoders.DOUBLE()).collectAsList().get(0);

        println("Avg length of tweets: " + avgLength);

        sparkSession.udf().register("extract_mentions", (String text)-> {
            final String[] tokens = text.split("\\s+");
            return Arrays.stream(tokens)
                    .filter(s-> s.startsWith("@"))
                    .collect(Collectors.toList());
        }, DataTypes.createArrayType(DataTypes.StringType));

        sparkSession.udf().register("extract_hashtags", (String text)-> {
            final String[] tokens = text.split("\\s+");
            return Arrays.stream(tokens)
                    .filter(s-> s.startsWith("#"))
                    .collect(Collectors.toList());
        }, DataTypes.createArrayType(DataTypes.StringType));



        final Dataset<Row> enriched = parsed
                .withColumn("mentions", expr("extract_mentions(text)"))
                .withColumn("hashtags", expr("extract_hashtags(text)"));

        final long countWithMentions = enriched.filter("size(mentions)>0").count();
        final long countWithHashtags = enriched.filter("size(hashtags)>0").count();

        println("Percent tweets with mentions: " + (100.0 * countWithMentions/totalCount));
        println("Percent tweets with hashtags: " + (100.0 * countWithHashtags/totalCount));

        println("----------------- top 3 user handles (user.screen_name) with the most number of tweets----------");
        enriched
                .groupBy("screen_name")
                .count()
                .orderBy(desc("count"))
                .limit(3)
                .show();

        enriched
                .select(explode(col("mentions")).alias("mention"))
                .groupBy("mention")
                .count()
                .orderBy(desc("count"))
                .limit(3)
                .show();

        enriched
                .select(explode(col("hashtags")).alias("hashtag"))
                .groupBy("hashtag")
                .count()
                .orderBy(desc("count"))
                .limit(3)
                .show();

        final Dataset<Row> retweets = parsed.filter("text like 'RT %'");
        final long recountCount = retweets.count();
        println("Percent of retweet counts: " + 100*recountCount/totalCount);

        println("-----------------  Count of tweets by week day -------------------------");
        parsed
                .selectExpr("from_unixtime(timestamp_ms, 'E') weekday")
                .groupBy("weekday")
                .count()
                .orderBy(desc("weekday"))
                .show();

    }
    public static void main(String[] args) throws ArgumentParserException {
        final TweetAnalysisApp stockApp = new TweetAnalysisApp();
        stockApp.start(args);
        stockApp.close();
    }
}

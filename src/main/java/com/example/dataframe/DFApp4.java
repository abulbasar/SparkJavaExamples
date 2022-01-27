package com.example.dataframe;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class DFApp4 {

    static class  WordDistanceUdf implements UDF3<String, String, String, Integer>{

        private Pattern wordSep;

        public WordDistanceUdf(){
            wordSep = Pattern.compile("\\s+");
        }

        @Override
        public Integer call(String text, String word1, String word2) throws Exception {
            Integer result = null;
            if(!(text == null || word1 == null || word2 == null)){
                if(word1.equals(word2)){
                    result = 0;
                }else { // 1. fast fast slow 2. fast fast fast
                    final String[] split = wordSep.split(text);
                    for (int i = 0; i < split.length; i++) {
                        if (split[i].equals(word1)) {
                            for (int j = 0; j < split.length; j++) {
                                if (split[j].equals(word2)) {
                                    int d = Math.abs(i - j) - 1;
                                    result = Math.min(result, d);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            return result;
        }
    }


    public void start(String ...args){
        SparkConf conf = new SparkConf();
        conf.setAppName(getClass().getName());
        conf.setMaster("local[4]"); // 4 executor threads
        conf.setIfMissing("spark.master", "local[4]");
        conf.setIfMissing("spark.default.parallelism", "16");
        conf.set("spark.hadoop.validateOutputSpecs", "false");
        conf.set("spark.eventLog.enabled", "true");
        conf.set("spark.eventLog.dir", "/tmp/spark-events-logs");

        final SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        /*
        sparkSession.udf().register("word_distance", (String text, String word1, String word2) -> {
            Integer result = null;
            if(!(text == null || word1 == null || word2 == null)){
                if(word1.equals(word2)){
                    result = 0;
                }else {
                    final String[] split = text.split("\\s+");
                    for (int i = 0; i < split.length; i++) {
                        if (split[i].equals(word1)) {
                            for (int j = 0; j < split.length; j++) {
                                if (split[j].equals(word2)) {
                                    result = Math.abs(i - j) - 1;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            return result;
        }, DataTypes.IntegerType);
        */

        sparkSession.udf().register("word_distance", new WordDistanceUdf(), DataTypes.IntegerType);


        final String[] testStrings = new String[]{
                "Fast text searching for regular expressions or automaton searching on tries",
                null,
                "hello world"
        };

        final Dataset<Row> dataset = sparkSession
                .createDataset(Arrays.asList(testStrings), Encoders.STRING())
                .toDF("value")
                ;

        final List<Integer> distances = dataset
                .selectExpr("word_distance(value, 'text', 'expressions')")
                .as(Encoders.INT())
                .collectAsList();


        assert distances.get(0) == 3;
        assert distances.get(1) == null;
        assert distances.get(2) == null;

        sparkSession.close();


    }


    public static void main(String ...args){
        new DFApp4().start(args);
    }


}

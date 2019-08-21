# SparkJavaExamples
Code of example of working with Apache Spark using Java


Build package
```
$ mvn package 
```

Copy the output jar to cluster and submit
```
$ ./bin/spark-submit \
    --class org.apache.spark.examples.SparkPi \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 4g \
    --executor-memory 16g \
    --executor-cores 4 \
    --queue default \
    --num-executors 4 \
    spark-examples*.jar \
    10

```

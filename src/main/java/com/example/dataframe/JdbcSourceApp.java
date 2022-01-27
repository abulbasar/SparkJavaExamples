package com.example.dataframe;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Closeable;
import java.io.IOException;

import static com.example.common.Utils.println;

public class JdbcSourceApp implements Closeable {

    // Command line arguments: --host 127.0.0.1 --port 3306 --database classicmodels --username root --password password123!

    private SparkConf sparkConf;
    private SparkSession sparkSession;


    public static void main(String[] args) throws Exception {
        final long startTime = System.currentTimeMillis();
        final JdbcSourceApp app = new JdbcSourceApp();
        app.start(args);
        app.close();
        final long duration = System.currentTimeMillis() - startTime;
        println(String.format("Process is complete. Took: %d secs", duration/1000));
    }

    private Namespace ns;

    public Namespace parseArguments(String[] args) throws Exception {
        final ArgumentParser parser = ArgumentParsers.newFor("prog").build()
                .description("Spark application for file format");

        parser.addArgument("-t", "--host")
                .help("JDBC host name")
                .setDefault("localhost")
                .required(false);

        parser.addArgument("-r", "--driver")
                .help("JDBC driver name")
                .setDefault("com.mysql.jdbc.Driver")
                .required(false);

        parser.addArgument("-p", "--port")
                .help("JDBC port number")
                .setDefault(3306)
                .type(Integer.class)
                .required(false);

        parser.addArgument("-d", "--database")
                .help("JDBC database name")
                .required(true);

        parser.addArgument("-u", "--username")
                .help("JDBC database username")
                .required(true);

        parser.addArgument("-w", "--password")
                .help("JDBC database password")
                .required(true);

        Namespace res = parser.parseArgs(args);
        return res;
    }

    private Dataset<Row> loadJdbcTable(String tableName){
        String host =  ns.getString("host");
        Integer port = ns.getInt("port");
        String database = ns.getString("database");
        String username = ns.getString("username");
        String password = ns.getString("password");
        String jdbcDriver = ns.getString("driver");

        final String dbUrl = String.format("jdbc:mysql://%s:%d/%s", host, port, database);
        println("Jdbc url: " + dbUrl);


        return sparkSession.read()
                .format("jdbc")
                .option("url", dbUrl)
                .option("driver", jdbcDriver)
                .option("dbtable", tableName)
                .option("user", username)
                .option("password", password)
                .load();

    }

    private void start(String[] args) throws Exception {
        ns = parseArguments(args);

        init();

        final Dataset<Row> customers = loadJdbcTable("customers");

        customers.show();


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


}

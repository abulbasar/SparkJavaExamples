package com.example.dataframe;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

import static org.apache.spark.sql.functions.expr;

public class MySqlRWApp extends SparkBaseApp{

    /*

    https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html


    Start a local mysql instance
    docker run -e MYSQL_ROOT_PASSWORD=password123! -p 3306:3306 -d mysql

    Download sample database from https://www.mysqltutorial.org/mysql-sample-database.aspx
    And import

    Command line argument: --host localhost --port 3306 --database classicmodels --username root --password password123!
    */

    private Namespace ns;

    public Namespace parseArguments(String[] args) throws ArgumentParserException {
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


        return sparkSession.read()
                .format("jdbc")
                .option("url", dbUrl)
                .option("driver", jdbcDriver)
                .option("dbtable", tableName)
                .option("user", username)
                .option("password", password)
                .load();

    }

    public void start(String[] args) throws ArgumentParserException {
        this.ns = parseArguments(args);

        initSparkSession();

        final Dataset<Row> customers = loadJdbcTable("customers");
        final Dataset<Row> orders = loadJdbcTable("orders");
        println("------------------------ customers table -----------------------");
        customers.show();

        println("------------------------ orders table -----------------------");
        orders.show();

        String ordersPath = "/tmp/mysql/orders";

        orders.write()
                .mode(SaveMode.Overwrite)
                .save(ordersPath);

        final Dataset<Row> ordersFs = sparkSession.read().load(ordersPath);


        println("---------------------- Join two tables : one from mysql and other from file system -------------------------");
        ordersFs.alias("t1")
                .join(customers.alias("t2"), expr("t1.customerNumber = t2.customerNumber"))
                .show();


    }
    public static void main(String[] args) throws ArgumentParserException {
        final MySqlRWApp stockApp = new MySqlRWApp();
        stockApp.start(args);
        stockApp.close();
    }
}

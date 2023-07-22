package org.example;

import org.apache.spark.sql.*;

public class Application1 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Tp Spark Sql")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> incidentsDF = spark.read()
                .option("header", true)
                .csv("incidents.csv");

        incidentsDF.createOrReplaceTempView("incidents");

        Dataset<Row> incidentsByService = spark.sql(
                "SELECT \"service\", COUNT(*) AS incident_count " +
                        "FROM incidents " +
                        "GROUP BY \"service\""
        );

        incidentsByService.show();

        spark.stop();
    }
}
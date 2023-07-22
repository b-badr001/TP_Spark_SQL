package org.example;

import org.apache.spark.sql.*;

public class Application2 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Tp Spark Sql")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> incidentsDF = spark.read()
                .option("header", true)
                .csv("incidents.csv");

        incidentsDF.createOrReplaceTempView("incidents");

        Dataset<Row> incidentsByYear = spark.sql(
                "SELECT SUBSTRING(date, 7) AS year, COUNT(*) AS incident_count " +
                        "FROM incidents " +
                        "GROUP BY SUBSTRING(date, 7) " +
                        "ORDER BY incident_count DESC " +
                        "LIMIT 2"
        );

        incidentsByYear.show();

        spark.stop();
    }
}

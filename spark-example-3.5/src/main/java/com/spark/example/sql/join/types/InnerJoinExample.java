package com.spark.example.sql.join.types;

import org.apache.spark.sql.SparkSession;

public class InnerJoinExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("InnerJoinExample")
                .enableHiveSupport()
                .getOrCreate();
    }
}

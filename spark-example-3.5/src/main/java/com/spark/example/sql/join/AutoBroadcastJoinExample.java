package com.spark.example.sql.join;

import org.apache.spark.sql.SparkSession;

public class AutoBroadcastJoinExample {
    public static void main(String[] args) {
        // 1. 创建 SparkSession
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("MapJoinExample")
                .config("spark.sql.autoBroadcasetJoinThreshold", "-1")
                .getOrCreate();

        spark.sql("");
    }
}

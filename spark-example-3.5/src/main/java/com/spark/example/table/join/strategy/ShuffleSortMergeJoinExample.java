package com.spark.example.table.join.strategy;

import org.apache.spark.sql.SparkSession;

public class ShuffleSortMergeJoinExample {
    public static void main(String[] args) {
        // 1. 创建 SparkSession
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("ShuffleSortMergeJoinExample")
                // 关闭 AQE 自动优化
                .config("spark.sql.adaptive", false)
                // 为了演示效果关闭 BroadcastJoin
                .config("spark.sql.autoBroadcastJoinThreshold", "-1")
                .enableHiveSupport()
                .getOrCreate();

        spark.sql("SELECT a1.province_id, a2.province_name, COUNT(*) AS order_num\n" +
                "FROM tb_order AS a1\n" +
                "JOIN tb_province AS a2\n" +
                "ON a1.province_id = a2.id\n" +
                "GROUP BY a1.province_id, a2.province_name").show();
    }
}

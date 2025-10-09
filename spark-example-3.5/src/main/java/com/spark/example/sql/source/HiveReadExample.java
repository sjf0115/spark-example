package com.spark.example.sql.source;

import org.apache.spark.sql.SparkSession;

import java.io.File;

/**
 * 功能：Spark SQL 读取 Hive
 * 作者：SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/10/9 下午9:14
 */
public class HiveReadExample {
    public static void main(String[] args) {
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Java Spark Hive Example")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();

        //  读取
        spark.sql("show databases").show();
        spark.sql("show tables").show();
        spark.sql("SELECT COUNT(*) FROM tb_order").show();
    }
}

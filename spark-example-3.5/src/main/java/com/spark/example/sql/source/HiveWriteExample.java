package com.spark.example.sql.source;

import org.apache.spark.sql.SparkSession;

import java.io.File;

/**
 * 功能：Spark SQL 写入 Hive
 * 作者：SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/10/9 下午9:14
 */
public class HiveWriteExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("HiveWriteExample")
                .enableHiveSupport()
                .getOrCreate();

        // 创建 Hive 表
        spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive");
        // 导入数据
        spark.sql("LOAD DATA LOCAL INPATH 'spark-example-3.5/src/main/resources/data/kv.txt' INTO TABLE src");

        // 查询
        spark.sql("SELECT * FROM src").show();
        // +---+-------+
        // |key|  value|
        // +---+-------+
        // |238|val_238|
        // | 86| val_86|
        // |311|val_311|
        // ...

        // 聚合
        spark.sql("SELECT COUNT(*) FROM src").show();
        // +--------+
        // |count(1)|
        // +--------+
        // |    500 |
        // +--------+
    }
}

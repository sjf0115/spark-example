package com.spark.example.sql.source;

import org.apache.spark.sql.SparkSession;

/**
 * 功能：Spark SQL 写入 Hive
 * 作者：SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/10/9 下午9:14
 */
public class HiveInsertExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("HiveWriteExample")
                .enableHiveSupport()
                .getOrCreate();

        // 创建 Hive 表
        spark.sql("CREATE TABLE IF NOT EXISTS tmp_insert (key INT, value STRING)");
        // 查询
        spark.sql("insert into tmp_insert values(1,'张三')").show();
    }
}

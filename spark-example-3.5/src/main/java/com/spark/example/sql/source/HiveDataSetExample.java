package com.spark.example.sql.source;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 功能：Spark SQL 读取 Hive 以 DataSet/DataFrame 形式返回
 * 作者：SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/10/9 下午9:14
 */
public class HiveDataSetExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("HiveDataSetExample")
                .enableHiveSupport()
                .getOrCreate();

        // DataFrame
        // SQL 查询的结果本身就是 dataframe，并支持所有正常功能。
        Dataset<Row> dataFrame = spark.sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key");
        dataFrame.show();
        /*+---+-----+
        |key|value|
        +---+-----+
        |  0|val_0|
        |  0|val_0|
        |  0|val_0|
        |  2|val_2|
        |  4|val_4|
        |  5|val_5|
        |  5|val_5|
        |  5|val_5|
        |  8|val_8|
        |  9|val_9|
        +---+-----+*/

        // DataSet
        Dataset<String> dataset = dataFrame.map(
                (MapFunction<Row, String>) row -> "Key: " + row.get(0) + ", Value: " + row.get(1),
                Encoders.STRING());
        dataset.show();

        /*+--------------------+
        |               value|
        +--------------------+
        |Key: 0, Value: val_0|
        |Key: 0, Value: val_0|
        |Key: 0, Value: val_0|
        |Key: 2, Value: val_2|
        |Key: 4, Value: val_4|
        |Key: 5, Value: val_5|
        |Key: 5, Value: val_5|
        |Key: 5, Value: val_5|
        |Key: 8, Value: val_8|
        |Key: 9, Value: val_9|
        +--------------------+*/
    }
}

package com.spark.example.sql.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TextFilesExample {
    public static void main(String[] args) {
        // 1. 创建 SparkSession
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("TextFilesExample")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        // A text dataset is pointed to by path.
        // The path can be either a single text file or a directory of text files
        String path = "spark-example-3.5/src/main/resources/data/people.txt";

        Dataset<Row> df1 = spark.read().text(path);
        df1.show();
        // +-----------+
        // |      value|
        // +-----------+
        // |Michael, 29|
        // |   Andy, 30|
        // | Justin, 19|
        // +-----------+

        // You can use 'lineSep' option to define the line separator.
        // The line separator handles all `\r`, `\r\n` and `\n` by default.
        Dataset<Row> df2 = spark.read().option("lineSep", ",").text(path);
        df2.show();
        // +-----------+
        // |      value|
        // +-----------+
        // |    Michael|
        // |   29\nAndy|
        // | 30\nJustin|
        // |       19\n|
        // +-----------+

        // You can also use 'wholetext' option to read each input file as a single row.
        Dataset<Row> df3 = spark.read().option("wholetext", "true").text(path);
        df3.show();
        //  +--------------------+
        //  |               value|
        //  +--------------------+
        //  |Michael, 29\nAndy...|
        //  +--------------------+

        // "output" is a folder which contains multiple text files and a _SUCCESS file.
        df1.write().text("output");

        // You can specify the compression format using the 'compression' option.
        df1.write().option("compression", "gzip").text("output_compressed");
    }
}

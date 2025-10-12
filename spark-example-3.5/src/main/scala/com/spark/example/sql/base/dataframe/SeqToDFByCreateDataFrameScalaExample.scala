package com.spark.example.sql.base.dataframe

import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
 * 从集合创建 DataFrame 通过 createDataFrame 方法
 */
object SeqToDFByCreateDataFrameScalaExample {

  def main( args: Array[String] ): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("SeqToDFByCreateDataFrameScalaExample")
      .master("local[*]")
      .getOrCreate()

    // Seq
    val seq = Seq(Row(1, 26000), Row(2, 30000), Row(4, 25000), Row(3, 20000))
    // 转换为 RDD
    val rdd = sparkSession.sparkContext.parallelize(seq)
    // Schema
    val schema = StructType(List(
      StructField("id", IntegerType, nullable = false),
      StructField("salary", IntegerType, nullable = false)
    ))

    val salariesDF = sparkSession.createDataFrame(rdd, schema)
    salariesDF.show()
    /*+---+------+
    | id|salary|
    +---+------+
    |  1| 26000|
    |  2| 30000|
    |  4| 25000|
    |  3| 20000|
    +---+------+*/
  }
}

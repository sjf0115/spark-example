package com.spark.example.sql.join.types

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object InnerJoinScalaExample {
  def main( args: Array[String] ): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("InnerJoinScalaExample")
      .setMaster("local[*]") // TODO 要打包提交集群执行，注释掉

    val sparkSession = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://localhost:8020")

    // 创建员工信息表
//    val seq = Seq((1, "Mike", 28, "Male"), (2, "Lily", 30, "Female"), (3, "Raymond", 26, "Male"), (5, "Dave", 36, "Male"))
//    val employees: DataFrame = seq.toDF("id", "name", "age", "gender")
//
//    // 创建薪资表
//    val seq2 = Seq((1, 26000), (2, 30000), (4, 25000), (3, 20000))
//    val salaries:DataFrame = seq2.toDF("id", "salary")

    val sqlstr =
      """
        |select
        |  sc.courseid,
        |  csc.courseid
        |from sale_course sc join course_shopping_cart csc
        |on sc.courseid=csc.courseid
      """.stripMargin

    sparkSession.sql("use sparktuning;")
    sparkSession.sql(sqlstr).show()

    while(true){}

  }
}
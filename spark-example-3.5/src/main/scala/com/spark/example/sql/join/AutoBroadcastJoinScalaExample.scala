package com.spark.example.sql.join

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AutoBroadcastJoinScalaExample {
  def main( args: Array[String] ): Unit = {
    val sparkConf = new SparkConf().setAppName("AutoBroadcastJoinExample")
      .set("spark.sql.autoBroadcastJoinThreshold","10m")
      .setMaster("local[*]") // TODO 要打包提交集群执行，注释掉

    System.setProperty("HADOOP_USER_NAME", "atguigu")
    val sparkSession = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    val ssc = sparkSession.sparkContext
    ssc.hadoopConfiguration.set("fs.defaultFS", "hdfs://localhost:8020")

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

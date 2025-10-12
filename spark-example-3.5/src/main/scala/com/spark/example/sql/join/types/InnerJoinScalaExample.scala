package com.spark.example.sql.join.types

import org.apache.spark.sql.{DataFrame, SparkSession}


object InnerJoinScalaExample {

  case class Employees(id: Int, name: String, age: Int, sex: String)

  def main( args: Array[String] ): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("InnerJoinScalaExample")
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    import sparkSession.implicits._

    // 创建员工信息表

    val employees = Seq(
      Employees(1, "Mike", 28, "Male"),
      Employees(2, "Lily", 30, "Female"),
      Employees(3, "Raymond", 26, "Male"),
      Employees(5, "Dave", 36, "Male")
    )
    val employeesDF = employees.toDF()

    // 创建薪资表
    val salariesSeq = Seq((1, 26000), (2, 30000), (4, 25000), (3, 20000))
    val salariesDF:DataFrame = salariesSeq.toDF("id", "salary")



    while(true){}

  }
}
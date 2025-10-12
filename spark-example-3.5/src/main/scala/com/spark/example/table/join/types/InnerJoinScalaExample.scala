package com.spark.example.table.join.types

import org.apache.spark.sql.{DataFrame, SparkSession}


object InnerJoinScalaExample {

  case class Employees(id: Int, name: String, age: Int, sex: String)

  def main( args: Array[String] ): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("InnerJoinScalaExample")
      .master("local[*]")
      .getOrCreate()

    import sparkSession.implicits._

    // 创建员工信息表
    val employees = Seq((1, "Mike", 28, "Male"), (2, "Lily", 30, "Female"), (3, "Raymond", 26, "Male"), (5, "Dave", 36, "Male"))
    val employeesDF:DataFrame = employees.toDF("id", "name", "age", "gender")

    // 创建薪资表
    val salariesSeq = Seq((1, 26000), (2, 30000), (4, 25000), (3, 20000))
    val salariesDF:DataFrame = salariesSeq.toDF("id", "salary")

    // 左表
    salariesDF.show
    /*+---+------+
    | id|salary|
    +---+------+
    |  1| 26000|
    |  2| 30000|
    |  4| 25000|
    |  3| 20000|
    +---+------+*/

    // 右表
    employeesDF.show
    /*+---+-------+---+------+
    | id|   name|age|gender|
    +---+-------+---+------+
    |  1|   Mike| 28|  Male|
    |  2|   Lily| 30|Female|
    |  3|Raymond| 26|  Male|
    |  5|   Dave| 36|  Male|
    +---+-------+---+------+*/

    // 内关联
    val jointDF: DataFrame = salariesDF.join(employeesDF, salariesDF("id") === employeesDF("id"), "inner")
    jointDF.show
    /*+---+------+---+-------+---+------+
    | id|salary| id|   name|age|gender|
    +---+------+---+-------+---+------+
    |  1| 26000|  1|   Mike| 28|  Male|
    |  2| 30000|  2|   Lily| 30|Female|
    |  3| 20000|  3|Raymond| 26|  Male|
    +---+------+---+-------+---+------+*/
  }
}
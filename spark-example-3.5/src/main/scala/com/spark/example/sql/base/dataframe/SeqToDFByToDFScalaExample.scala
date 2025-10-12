package com.spark.example.sql.base.dataframe

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 从集合创建 DataFrame 通过 toDF 方法
 */
object SeqToDFByToDFScalaExample {

  case class Employees(id: Int, name: String, age: Int, sex: String)

  def main( args: Array[String] ): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("SeqToDataFrameScalaExample")
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    // 注意：使用 toDF 需要引入 SparkSession示例的隐式转换
    import sparkSession.implicits._

    // 1. 从元组序列创建薪资表
    val salariesSeq = Seq((1, 26000), (2, 30000), (4, 25000), (3, 20000))
    val salariesDF: DataFrame = salariesSeq.toDF("id", "salary")
    salariesDF.show()
    /*+---+------+
    | id|salary|
    +---+------+
    |  1| 26000|
    |  2| 30000|
    |  4| 25000|
    |  3| 20000|
    +---+------+*/

    // 2. 从样例类序列创建员工信息表
    val employeesSeq = Seq(
      Employees(1, "Mike", 28, "Male"),
      Employees(2, "Lily", 30, "Female"),
      Employees(3, "Raymond", 26, "Male"),
      Employees(5, "Dave", 36, "Male")
    )
    val employeesDF: DataFrame = employeesSeq.toDF()
    employeesDF.show()
    /*+---+-------+---+------+
    | id|   name|age|   sex|
    +---+-------+---+------+
    |  1|   Mike| 28|  Male|
    |  2|   Lily| 30|Female|
    |  3|Raymond| 26|  Male|
    |  5|   Dave| 36|  Male|
    +---+-------+---+------+*/
  }
}

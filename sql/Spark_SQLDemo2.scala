package practice01.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark_SQLDemo2 {
  def main(args: Array[String]): Unit = {
    //SaprkConf
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_sql")
    //SparkSession
    val spark:SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //  读取数据
    val frame: DataFrame = spark.read.json("in/user.json")

    //将dataFrame转换为一张表
    frame.createOrReplaceTempView("user")
    //采用sql的语法访问数据
    spark.sql("select * from user").show

    spark.stop
  }
}

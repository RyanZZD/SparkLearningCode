package practice01.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Spark_SQLDemo01 {
  def main(args: Array[String]): Unit = {
    //SaprkConf
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_sql")
    //SparkSession
//    创建sparkSQL的环境对象
//    val spark: SparkSession = new SparkSession(sparkConf)
    val spark:SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
//  读取数据
    val frame: DataFrame = spark.read.json("in/user.json")
//  展示数据
    frame.show()
//    释放资源
    spark.stop
  }
}

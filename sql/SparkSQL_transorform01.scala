package practice01.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL_transorform01 {
  def main(args: Array[String]): Unit = {
    //SaprkConf
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_sql")
    //SparkSession
    val spark:SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //进行转化之前，需要引入隐式转换规则
    //这里的spark不是包名的含义，是sparkSession对象的名字
    import spark.implicits._

    //  创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhagnsan", 20),(2, "lisi", 30),(3, "wangwu", 40)))

    //RDD - DataSet
    val userRDD:RDD[User] = rdd.map{
      case (id, name, age)=>{
        User(id, name, age)
      }
    }
    val userDS: Dataset[User] = userRDD.toDS()
    val rdd1:RDD[User] = userDS.rdd
    rdd1.foreach(println)
    spark.stop
  }
}

package practice01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark_sortBy {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.parallelize(List(2, 1, 4, 3, 0))
    //按照自身大小排序
//    listRDD.sortBy(x=>x).collect().foreach(println)

    //按照与3余数的大小排序
    listRDD.sortBy(x=>x%3).collect().foreach(println)

    sc.stop()
  }
}
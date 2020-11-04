package practice01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark_zip {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val rdd1: RDD[Int] = sc.makeRDD(Array(1, 2, 3), 3)
    val rdd2: RDD[String] = sc.parallelize(Array("a", "b", "c"), 3)
    val rdd: RDD[(Int, String)] = rdd1.zip(rdd2)
    rdd.collect().foreach(println)
    sc.stop()
  }
}

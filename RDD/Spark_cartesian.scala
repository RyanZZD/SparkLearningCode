package practice01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark_cartesian {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val rdd1: RDD[Int] = sc.makeRDD(1 to 3)
    val rdd2: RDD[Int] = sc.parallelize(2 to 5)
    val rdd: RDD[(Int, Int)] = rdd1.cartesian(rdd2)
    rdd.collect().foreach(println)
    sc.stop()
  }
}

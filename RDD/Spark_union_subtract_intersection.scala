package practice01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark_union_subtract_intersection {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val rdd1: RDD[Int] = sc.makeRDD(1 to 5)
    val rdd2: RDD[Int] = sc.parallelize(3 to 8)
    val rdd3: RDD[Int] = rdd1.union(rdd2)
    val rdd4: RDD[Int] = rdd1.subtract(rdd2)
    val rdd5: RDD[Int] = rdd1.intersection(rdd2)
    rdd3.collect().foreach(println)
    println("-----------------------")
    rdd4.collect().foreach(println)
    println("-----------------------")
    rdd5.collect().foreach(println)
    sc.stop()
  }
}

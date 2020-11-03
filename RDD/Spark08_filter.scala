package practice01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark08_filter {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //过滤。返回一个新的RDD，该RDD由经过func函数计算后返回值为true的输入元素组成。
    val filterRDD: RDD[Int] = listRDD.filter(i => (i % 2)==0)

    filterRDD.collect().foreach(println)

  }
}

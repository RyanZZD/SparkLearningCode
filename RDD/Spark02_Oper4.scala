package practice01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark02_Oper4 {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val listRDD: RDD[List[Int]] = sc.makeRDD(Array(List(1, 2), List(3, 4)))

    //flatMap
    //1, 2, 3, 4
    val flatMapRDD: RDD[Int] = listRDD.flatMap(datas=>datas)
    flatMapRDD.collect().foreach(println)
  }
}

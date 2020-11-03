package practice01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark02_Oper3 {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    //map算子
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10, 2)


    //创建一个RDD，使每个元素跟所在分区形成一个元组组成一个新的RDD
    val tupleRDD: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex {
      case (num, datas) => {
        datas.map((_, "分区号: " + num))
      }
    }
    tupleRDD.collect().foreach(println)
  }
}

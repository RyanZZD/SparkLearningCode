package practice01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Oper2 {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    //map算子
    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    //mapPartitionsRDD可以对一个RDD中所有的分区进行遍历
    //mapPartitionsRDD效率由于map算子，减少了发送到执行器执行交互次数
    //mapPartitionsRDD可能会出现内存溢出（OOM)
    val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(datas => {
      datas.map(data => data * 2)
    })
    mapPartitionsRDD.collect().foreach(println)
  }
}

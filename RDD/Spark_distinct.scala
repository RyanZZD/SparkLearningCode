package practice01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark_distinct {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 5, 2, 9, 6, 1))

    //使用distinct算子对数据去重，但是也为去重后会导致数据减少，所以可以改变默认的分区的数量
    //    val distinctRDD: RDD[Int] = listRDD.distinct()
    val distinctRDD: RDD[Int] = listRDD.distinct(2) //用两个分区保存结果
    //    distinctRDD.collect().foreach(println)
    distinctRDD.saveAsTextFile("output")
  }
}

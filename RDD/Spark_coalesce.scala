package practice01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark_coalesce {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(0 to 16, 4)
//    缩减分区数，用于大数据集过滤后，提高小数据集的执行效率
    println("缩减分区前 = " + listRDD.partitions.size)

    val coalesceRDD: RDD[Int] = listRDD.coalesce(3)
    println("缩减分区后 = " + coalesceRDD.partitions.size)
  }
}

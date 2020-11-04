package practice01.key_value

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

//自定义分区器
object spark_partitionBy {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

//    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 1, 5, 2, 9, 6, 1))
    val listRDD = sc.makeRDD(List(("a", 1), ("b", 2),("c", 3)))
    val partRDD: RDD[(String, Int)] = listRDD.partitionBy(new myPraitioner(3))
    partRDD.saveAsTextFile("output1")

    sc.stop()
  }
}

//声明分区器
//
class myPraitioner(partitions: Int) extends Partitioner{
  override def numPartitions: Int = {
      partitions
  }
  override def getPartition(key: Any): Int = {
    1
  }
}
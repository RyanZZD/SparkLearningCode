package practice01.key_value

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_groupBy {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val wordRDD = sc.makeRDD(Array("one", "two", "three", "three", "two", "three"))
    val wordPairsRDD:RDD[(String, Int)] = wordRDD.map(word => (word, 1))
    val groupRDD: RDD[(String, Iterable[Int])]= wordPairsRDD.groupByKey()
    groupRDD.collect().foreach(println)
    groupRDD.map(t => (t._1, t._2.sum)).collect().foreach(println)
    sc.stop()
  }
}

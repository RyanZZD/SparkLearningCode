package practice01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark07_groupBy {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //生成数据，按照指定的规则进行分组
    //分组后的数据形成了对偶元组(K-V)， K表示分组的key，v表示分组的数据集合
    val groupByRDD: RDD[(Int, Iterable[Int])] = listRDD.groupBy(i => i % 2)

    groupByRDD.collect().foreach(println)
  }
}

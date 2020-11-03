package practice01

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark09_sample {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)
    //从指定的数据集合中进行抽样处理，根据不同的算法进行抽样
    // 随机种子随机抽样出数量为fraction的数据，withReplacement表示是抽出的数据是否放回，
    // true为有放回的抽样，false为无放回的抽样，seed用于指定随机数生成器种子。
    val sampleRDD: RDD[Int] = listRDD.sample(true, 0.4,1)

    sampleRDD.collect().foreach(println)
  }
}

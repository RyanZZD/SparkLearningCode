package practice01.key_value

import org.apache.spark.{SparkConf, SparkContext}

object Spark_foldByKey {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)
    //创建一个pairRDD，取出每个分区相同key对应值的最大值，然后相加
    val rdd = sc.parallelize(List(("a",3),("a",2),("c",4),("b",3),("c",6),("c",8)),2)
    //    rdd.glom().collect().foreach(println)
    val fold = rdd.foldByKey(0)(_+_)
    fold.collect().foreach(println)

    sc.stop()
  }
}

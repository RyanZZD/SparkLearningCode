package practice01.key_value

import org.apache.spark.{SparkConf, SparkContext}

object Spark_sortByKey {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val rdd = sc.parallelize(Array((3,"aa"),(6,"cc"),(2,"bb"),(1,"dd")))

    val result = rdd.sortByKey(true)
    result.collect().foreach(println)
    sc.stop()
  }
}

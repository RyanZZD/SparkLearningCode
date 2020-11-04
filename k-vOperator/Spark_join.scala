package practice01.key_value

import org.apache.spark.{SparkConf, SparkContext}

object Spark_join {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val rdd = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c")))
    val rdd1 = sc.parallelize(Array((1,4),(2,5),(3,6)))
    val result =  rdd.join(rdd1)

    /*输出
      (1,(a,4))
      (2,(b,5))
      (3,(c,6))
    * */
    result.collect().foreach(println)
    sc.stop()

  }
}

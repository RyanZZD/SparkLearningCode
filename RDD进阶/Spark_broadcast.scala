package practice01.RDD2

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_broadcast {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建Spark上下文对象
    val sc = new SparkContext(config)

    val rdd1 = sc.parallelize(Array((1,"a"),(2,"b"),(3,"c")))
    val list = List((1,4),(2,5),(3,6))

//    可以使用广播变量减少数据的传输
//    1.构建广播变量
    val broadcast: Broadcast[List[(Int, Int)]] = sc.broadcast(list)

    val resultRDD: RDD[(Int, (String, Any))] = rdd1.map{
      case (key, value) => {
        var v2: Any = null
//        使用广播变量
        for (t <- broadcast.value){
          if(key == t._1){
            v2 = t._2
          }
        }
        (key, (value, v2))
      }
    }

    resultRDD.foreach(println)
    sc.stop()
  }
}

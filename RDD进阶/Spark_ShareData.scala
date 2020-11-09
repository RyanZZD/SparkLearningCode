package practice01.RDD2

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator

object Spark_ShareData {
  def main(args: Array[String]): Unit = {
    //1.初始化配置信息及SparkContext
    val config: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(config)

    //2.创建一个RDD
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3,4),2)

//    val i:Int = rdd.reduce(_+_)
//    println(i)
    var sum: Int = 0
//    使用累加器来共享变量，来累加数据
//    创建累加器对象
    val accumulator: LongAccumulator  = sc.longAccumulator

//    rdd.foreach(i => sum=sum+i)
    rdd.foreach{
      case i => {
//        执行累加器的累加功能
        accumulator.add(i)
      }
    }
    println("sum = " + accumulator.value)
    sc.stop()
  }
}

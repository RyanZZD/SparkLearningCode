package practice01.RDD2

import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}

//自定义累加器
object Spark_Accumulator {
  def main(args: Array[String]): Unit = {
    //1.初始化配置信息及SparkContext
    val config: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(config)

    //2.创建一个RDD
    val rdd: RDD[String] = sc.makeRDD(List("hadoop", "hive","scala","hbase","spark","sql"),2)

//    创建累加器
    val wordAccumulator = new WordLongAccumulator
//  注册累加器
    sc.register(wordAccumulator)

    rdd.foreach{
      case word => {
        //        执行累加器的累加功能
        wordAccumulator.add(word)
      }
    }
    println("sum = " + wordAccumulator.value)
    sc.stop()
  }
}

//声明累加器
//1.继承AccumulatorV2
//2.实现抽象方法
//3.创建累加器
class WordLongAccumulator extends AccumulatorV2[String, util.ArrayList[String]] {

  val list= new util.ArrayList[String]()

  //当前的累加器是否是初始化状态
  override def isZero: Boolean = {
    list.isEmpty
  }

  //  复制累加器对象
  override def copy(): AccumulatorV2[String, util.ArrayList[String]] = {
    new WordLongAccumulator()
  }

  //重置累加器对象
  override def reset(): Unit = {
    list.clear()
  }

  //向累加器中增加数据
  override def add(v: String): Unit = {
    if ( v.contains("h")){
      list.add(v)
    }
  }

  //合并
  override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
    list.addAll(other.value)
  }

  //获取累加器的结果
  override def value: util.ArrayList[String] = {
    list
  }
}
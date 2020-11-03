package practice01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //使用开发工具完成Spark WordCount开发

    //local模式
    //创建sparkConf对象
    //设定spark计算框架的运行(部署)环境
    //aPP id
    val config: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    //创建spark上下问对象
    val sc = new SparkContext(config)
//    println(sc)
    //读取文件,将文件内容一行一行的读取出来
    val lines: RDD[String] = sc.textFile("in/word")
    //将一行一行的数据分解为一个一个的单词
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //为了统计方便，将单词数据进行我们的结构转换
    val wordToOne:RDD[(String, Int)] =words.map((_,1))

    //对转换结构后的数据进行分组聚合
    val wordToSum:RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
    //将统计结果采集后打印到控制台
    val result: Array[(String, Int)] = wordToSum.collect()
//    println(result)
    result.foreach(println)
  }
}


//输出结果:
// (scala,2)
//(hello,2)
//(world,1)
//(spark,2)
//(hadoop,1)
//(hi,4)
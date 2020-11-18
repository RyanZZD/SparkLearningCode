package practice01.sparkStream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
object wordCount {
  def main(args: Array[String]): Unit = {
    //使用sparkStream完成WordCount

    //Spark配置对象
    val sparkConf=new SparkConf().setMaster("local[*]").setAppName("SparkStramining_wordCount")

    //实时数据分析环境对象
    //采集周期，以指定的时间为周期采集实时数据
    val streamingContext = new StreamingContext(sparkConf, Seconds(6))
    //从指定的端口中采集数据
    val socketLineDStream:ReceiverInputDStream[String] = streamingContext.socketTextStream("ryan", 9999)
    //将采集的数据进行分解（扁平化）
    val wordDStream:DStream[String] = socketLineDStream.flatMap(line => line.split(" "))
    //将数据进行结构的转换，方便统计分析
    val mapDStream: DStream[(String, Int)] = wordDStream.map((_, 1))

    //将转换结构后的数据进行聚合处理
    val wordToSumDStream: DStream[(String, Int)] = mapDStream.reduceByKey(_+_)

    //打印结果
    wordToSumDStream.print()
    //不能停止采集程序
//    streamingContext.stop()
    //启动采集器
    streamingContext.start()
    //Driver等待采集器的执行
    streamingContext.awaitTermination()
  }
}

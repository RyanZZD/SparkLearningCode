package practice01.sql

import org.apache.spark.{Aggregator, SparkConf}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, Row, SparkSession, TypedColumn}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

object SparkSQL_UDAF_Class {
  def main(args: Array[String]): Unit = {
    //SaprkConf
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_sql")
    //SparkSession
    val spark:SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //进行转化之前，需要引入隐式转换规则
    //这里的spark不是包名的含义，是sparkSession对象的名字
    import spark.implicits._

    //自定义聚合函数
    //创建聚合函数对象
    val udaf = new MyAgeAvgFunction1
    //将聚合函数转换为查询列
    val avgCol: TypedColumn[UserBean, Double] = udaf.toColumn.name("avgAge")
    //  读取数据
    val frame: DataFrame = spark.read.json("in/user.json")
    val userDS: Dataset[UserBean] = frame.as[UserBean]

    //应用函数
    userDS.select(avgCol).show
    spark.stop
  }
}

case class UserBean( name:String, age: BigInt)
case class AvgBuffer(var sum: BigInt, var count: Int)

//声明用户自定义聚合函数（强类型）
// 1）继承Aggregator,设定泛型
// 2）实现方法
//Aggregator有两个，注意选择合适的类型
class MyAgeAvgFunction1 extends Aggregator[UserBean, AvgBuffer ,Double] {
  //初始化
  override def zero: AvgBuffer = {
    AvgBuffer(0, 0)
  }
  //聚合数据
  override def reduce(b: AvgBuffer, a:UserBean): AvgBuffer = {
    b.sum = b.sum + a.age
    b.count = b.count + 1
    b
  }
  //缓冲区的合并操作
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer={
    b1.sum = b1.sum + b2.sum
    b1.count = b1.count + b2.count
    b1
  }
  //完成计算
  override def finish(reduction: AvgBuffer):Double = {
    reduction.sum.toDouble / reduction.count
  }
  override def bufferEncoder:Encoder[AvgBuffer]={
    Encoders.product
  }
  override def outputEncoder: Encoder[Double] = {
    Encoders.scalaDouble
  }
}

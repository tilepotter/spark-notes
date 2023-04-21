package com.wyk.spark.sql.spark_sql

import com.wyk.spark.sql.spark_sql.UDAF.MyAvgUDAF
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}

/**
 * @ObjectName UDAF2
 * @Author wangyingkang
 * @Date 2022/5/30 17:17
 * @Version 1.0
 * @Description
 * */
object UDAF2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark-SQL").master("local[*]").getOrCreate()

    val df: DataFrame = spark.read.option("multiline", "true").json("input/json/emp.json")

    df.createOrReplaceTempView("emp")

    //spark.udf.register("avgAge", functions.udaf(new MyAvgUDAF))

    //使用UDAF函数
    spark.sql("select  avgAge(age) from emp").show()

    spark.close()
  }

  case class Buff(var total: Long, var count: Long)

  /*
    自定义聚合函数类：计算年龄的平均值
    1. 继承org.apache.spark.sql.expressions.Aggregator, 定义泛型
        IN : 输入的数据类型 Long
        BUF : 缓冲区的数据类型 Buff
        OUT : 输出的数据类型 Long
    2. 重写方法(6)
    */
  class MyAvgUDAF extends Aggregator[Long, Buff, Long] {
    // z & zero : 初始值或零值
    // 缓冲区的初始化
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    // 根据输入的数据更新缓冲区的数据
    override def reduce(buf: Buff, input: Long): Buff = {
      buf.total = buf.total + input
      buf.count = buf.count + 1
      buf
    }

    // 合并缓冲区的数据
    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.total = b1.total + b2.total
      b1.count = b1.count + b2.count
      b1
    }

    //计算结果
    override def finish(reduction: Buff): Long = {
      reduction.total / reduction.count
    }

    // 缓冲区的编码操作
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    // 输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }
}

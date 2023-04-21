package com.wyk.spark.sql.spark_sql

import com.wyk.spark.sql.entity.Emp
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession, TypedColumn}

/**
 * @ObjectName UDAF3
 * @Author wangyingkang
 * @Date 2022/5/31 17:22
 * @Version 1.0
 * @Description
 * */
object UDAF3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark-SQL").master("local[*]").getOrCreate()

    import spark.implicits._

    val df: DataFrame = spark.read.option("multiline", "true").json("input/json/emp.json")


    // 早期版本中，spark不能在sql中使用强类型UDAF操作
    // SQL & DSL
    // 早期的UDAF强类型聚合函数使用DSL语法操作
    val ds: Dataset[Emp] = df.as[Emp]

    val udafCol: TypedColumn[Emp, Long] = new MyAvgUDAF().toColumn

    ds.select(udafCol).show()

    spark.close()

  }

  case class Buff(var total: Long, var count: Long)

  /*
     自定义聚合函数类：计算年龄的平均值
     1. 继承org.apache.spark.sql.expressions.Aggregator, 定义泛型
         IN : 输入的数据类型 User
         BUF : 缓冲区的数据类型 Buff
         OUT : 输出的数据类型 Long
     2. 重写方法(6)
     */
  class MyAvgUDAF extends Aggregator[Emp, Buff, Long] {
    override def zero: Buff = {
      Buff(0L, 0L)
    }

    // 根据输入的数据更新缓冲区的数据
    override def reduce(buffer: Buff, input: Emp): Buff = {
      buffer.total = buffer.total + input.age
      buffer.count = buffer.count + 1
      buffer
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

    override def bufferEncoder: Encoder[Buff] = Encoders.product

    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }
}

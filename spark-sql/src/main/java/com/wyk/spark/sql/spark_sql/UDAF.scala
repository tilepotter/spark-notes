package com.wyk.spark.sql.spark_sql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @ObjectName UDAF
 * @Author wangyingkang
 * @Date 2022/5/30 16:52
 * @Version 1.0
 * @Description 用户可以自定义聚合函数。通过继承 UserDefinedAggregateFunction 来实现用户自定义弱类型聚合函数:;
 *              从 Spark3.0 版本 后，UserDefinedAggregateFunction 已经不推荐使用
 * */
object UDAF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark-SQL").master("local[*]").getOrCreate()

    val df: DataFrame = spark.read.option("multiline", "true").json("input/json/emp.json")

    df.createOrReplaceTempView("emp")

    spark.udf.register("avgAge", new MyAvgUDAF())

    //使用UDAF函数
    spark.sql("select  avgAge(age) from emp").show()

    spark.close()
  }

  /*
     自定义聚合函数类：计算年龄的平均值
     1. 继承UserDefinedAggregateFunction
     2. 重写方法(8)
     */
  class MyAvgUDAF extends UserDefinedAggregateFunction {

    // 输入数据的结构 : Int
    override def inputSchema: StructType = {
      StructType(
        Array(
          StructField("age", LongType)
        )
      )
    }

    // 缓冲区数据的结构 : Buffer
    override def bufferSchema: StructType = {
      StructType(
        Array(
          StructField("total", LongType),
          StructField("count", LongType)
        )
      )
    }

    // 函数计算结果的数据类型：Out
    override def dataType: DataType = LongType

    // 函数的稳定性
    override def deterministic: Boolean = true

    // 缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, 0L)
      buffer.update(1, 0L)
    }

    // 根据输入的值更新缓冲区数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer.update(0, buffer.getLong(0) + input.getLong(0))
      buffer.update(1, buffer.getLong(1) + 1)
    }

    // 缓冲区数据合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
      buffer1.update(1, buffer1.getLong(1) + buffer2.getLong(1))
    }

    // 计算平均值
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0) / buffer.getLong(1)
    }
  }
}

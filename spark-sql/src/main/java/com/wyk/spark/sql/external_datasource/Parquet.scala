package com.wyk.spark.sql.external_datasource

import org.apache.spark.sql.SparkSession

/**
 * @ObjectName Parquet
 * @Author wangyingkang
 * @Date 2022/4/13 16:17
 * @Version 1.0
 * @Description Parquet 是一个开源的面向列的数据存储，它提供了多种存储优化，允许读取单独的列非整个文件，
 *              这不仅节省了存储空间而且提升了读取效率，它是 Spark 默认的文件格式。
 * */
object Parquet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark-SQL").master("local[2]").getOrCreate()

    //读取parquet文件
    val df=spark.read.format("parquet").load("input/parquet/emp.parquet").toDF()

    df.show()
  }
}

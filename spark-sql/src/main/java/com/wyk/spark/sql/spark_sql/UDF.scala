package com.wyk.spark.sql.spark_sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @ObjectName UDF
 * @Author wangyingkang
 * @Date 2022/5/30 10:02
 * @Version 1.0
 * @Description SparkSQL UDF函数：通过 spark.udf 功能添加自定义函数，实现自定义功能
 * */
object UDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark-SQL").master("local[*]").getOrCreate()

    val df: DataFrame = spark.read.option("multiline", "true").json("input/json/emp.json")

    df.createOrReplaceTempView("emp")

    spark.udf.register("prefixName", (name: String) => {
      "Name: " + name
    })

    spark.sql("select age,prefixName(ename) from emp").show()

    spark.close()
  }
}

package com.wyk.spark.sql.external_datasource

import org.apache.spark.sql.SparkSession

/**
 * @ObjectName Json
 * @Author wangyingkang
 * @Date 2022/4/13 16:02
 * @Version 1.0
 * @Description 本例使用spark-sql 读取json格式的数据
 *              默认不支持一条数据记录跨越多行，可以通过配置 multiLine 为 true 来进行更改，其默认值为 false
 * */
object Json {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark-SQL").master("local[2]").getOrCreate()

    //读取json格式的数据
    val df = spark.read.format("json").option("multiline", "true").load("input/json/emp.json").toDF()

    df.show()

    //将json格式的数据写到指定路径
    df.write.format("json").save("output/json/emp.json")

  }
}

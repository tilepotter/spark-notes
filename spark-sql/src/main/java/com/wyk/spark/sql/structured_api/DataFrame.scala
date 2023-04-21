package com.wyk.spark.sql.structured_api
// 建议在进行 spark SQL 编程前导入下面的隐式转换，因为 DataFrames 和 dataSets 中很多操作都依赖了隐式转换

import org.apache.spark.sql.{DataFrame, SparkSession}
/**
 * @ObjectName DataFrame
 * @Author wangyingkang
 * @Date 2022/4/12 15:33
 * @Version 1.0
 * @Description
 * */
object DataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark-SQL").master("local[*]").getOrCreate()

    import spark.implicits._
    val df: DataFrame =spark.read.option("multiline","true").json("input/json/emp.json").cache()

    df.show()

    df.select("age","depno","ename","gender").filter($"age">15).show()
  }
}

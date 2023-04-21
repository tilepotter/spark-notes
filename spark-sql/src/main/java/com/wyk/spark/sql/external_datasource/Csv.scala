package com.wyk.spark.sql.external_datasource

import com.wyk.spark.sql.entity.Emp
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
 * @ObjectName Csv
 * @Author wangyingkang
 * @Date 2022/4/13 10:04
 * @Version 1.0
 * @Description CSV 是一种常见的文本文件格式，其中每一行表示一条记录，记录中的每个字段用逗号分隔。
 *              本例使用spark-sql 读取csv格式的数据
 * */
object Csv {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark-SQL").master("local[2]").getOrCreate()

    /**
     * 1.1 使用自动推断类型读取CSV文件
     */
    spark.read.format("csv")
      .option("header", "false") // 文件中的第一行是否为列的名称
      .option("mode", "FAILFAST") //读取模式：遇到格式不正确的数据时立即失败
      .option("inferSchema", "true") // 是否自动推断 schema
      .load("input/csv/dept.csv") //加载路径
      .show()

    /**
     * 1.2 使用预定义类型：
     */
    //预定义数据格式
    val deptSchema = new StructType(Array(
      StructField("deptno", LongType, nullable = true),
      StructField("dname", StringType, nullable = true),
      StructField("loc", StringType, nullable = true)
    ))

    //以预定义格式读取csv数据
    val df: DataFrame =spark.read.format("csv")
      .option("mode", "FAILFAST") //读取模式：遇到格式不正确的数据时立即失败
      .schema(deptSchema)
      .load("input/csv/dept.csv")
      .toDF()

    /**
     * 2.1 写入CSV文件
     */

    df.write.format("csv").mode("overwrite").save("output/csv/emp")

    //也可以指定具体的分隔符：
    df.write.format("csv").mode("overwrite").option("seq", "\t").save("output/csv/emp2")

  }
}

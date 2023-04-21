package com.wyk.spark.sql.structured_api

import com.wyk.spark.sql.entity.Dep
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types._

/**
 * @ObjectName DataFrame_RDD
 * @Author wangyingkang
 * @Date 2022/4/12 16:40
 * @Version 1.0
 * @Description Spark 支持两种方式把 RDD 转换为 DataFrame，分别是使用反射推断和指定 Schema 转换
 * */
object RDD2DataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark-SQL").master("local[*]").getOrCreate()
    // 1.需要导入隐式转换
    import spark.implicits._

    /**
     * 1. 使用反射推断
     */
    val rdd: RDD[Dep] = spark.sparkContext.textFile("input/txt//dept.txt")
      .map(_.split("\t"))
      .map(line => Dep(line(0).trim, line(1), line(2)))

    val rdd2DF: DataFrame = rdd.toDF()

    rdd2DF.select($"depno", $"dname", $"loc").show()
    println("<=============================================>")

    /**
     * 2. 以编程方式指定Schema
     */
    // 2.1 定义每个列的列类型
    val fields: Array[StructField] = Array(StructField("depno", StringType, nullable = true),
      StructField("dname", StringType, nullable = true),
      StructField("loc", StringType, nullable = true))

    //2.2 创建schema
    val schema: StructType = StructType(fields)

    //2.3 创建RDD
    val deptRDD: RDD[String] = spark.sparkContext.textFile("input/txt/dept.txt")

    val rowRDD: RDD[Row] = deptRDD.map(_.split("\t")).map(line => Row(line(0).trim, line(1), line(2)))

    //2.4 将RDD转换为DataFrame
    val deptDF: DataFrame = spark.createDataFrame(rowRDD, schema)

    deptDF.select('depno, 'dname, 'loc).show()
  }
}

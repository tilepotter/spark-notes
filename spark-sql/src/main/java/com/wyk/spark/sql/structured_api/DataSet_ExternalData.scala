package com.wyk.spark.sql.structured_api

import com.wyk.spark.sql.entity.Emp
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @ClassName DataSet_ExternalData
 * @Author wangyingkang
 * @Date 2022/4/12 16:01
 * @Version 1.0
 * @Description 外部数据创建DataSet
 *              DataFrame => DataSet df.as[样例类]
 * */
object DataSet_ExternalData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark-SQL").master("local[2]").getOrCreate()
    // 1.需要导入隐式转换
    import spark.implicits._

    //2.由外部数据集创建 Dataset
    //默认不支持一条数据记录跨越多行 (如下)，可以通过配置 multiLine 为 true 来进行更改，其默认值为 false
    val df: DataFrame = spark.read.option("multiline", "true").json("input/json/emp.json")
    val ds: Dataset[Emp] = df.as[Emp]
    ds.show()

    ds.select("ename", "age", "gender").filter($"age" > 15).show()
  }
}




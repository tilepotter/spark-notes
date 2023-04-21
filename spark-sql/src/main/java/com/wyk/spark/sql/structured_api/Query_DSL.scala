package com.wyk.spark.sql.structured_api

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{asc, desc}

/**
 * @ObjectName Query
 * @Author wangyingkang
 * @Date 2022/4/12 17:05
 * @Version 1.0
 * @Description 使用Structured API进行基本查询
 * */
object Query_DSL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark-SQL").master("local[2]").getOrCreate()
    // 1.需要导入隐式转换
    import spark.implicits._

    val df = spark.read.option("multiline", "true").json("input/json/emp.json").cache()

    // 1.查询指定字段：员工姓名及性别
    df.select($"ename", $"gender").show()

    //2.filter 查询工资大于2000 的员工信息
    df.filter($"sal" > 2000).show()

    // 3.orderBy 按照部门编号降序，工资升序进行查询
    df.orderBy(desc("depno"), asc("sal")).show()

    // 4.limit 查询工资最高的 3 名员工的信息
    df.orderBy(desc("sal")).limit(3).show()

    // 5.distinct 查询所有部门编号
    df.select("depno").distinct().show()

    // 6.groupBy 分组统计部门人数
    df.groupBy($"depno").count().show()

    //7. groupBY 分组统计男女性别人数
    df.groupBy($"gender").count().show()

    //8. groupBy 分组统计每个部门最高工资
    df.groupBy($"depno").max("sal").show()
  }
}

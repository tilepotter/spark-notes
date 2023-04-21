package com.wyk.spark.sql.spark_sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @ObjectName Query
 * @Author wangyingkang
 * @Date 2022/4/12 17:20
 * @Version 1.0
 * @Description 创建临时视图，使用spark-sql进行查询
 * */
object Query {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark-SQL").master("local[*]").getOrCreate()

    //默认不支持一条数据记录跨越多行 (如下)，可以通过配置 multiLine 为 true 来进行更改，其默认值为 false
    val df: DataFrame = spark.read.option("multiline", "true").json("input/json/emp.json").cache()

    // 1.首先需要将 DataFrame 注册为临时视图
    /**
     * 1.1 使用 createOrReplaceTempView 创建的是会话临时视图，它的生命周期仅限于会话范围，会随会话的结束而结束。
     * 1.2 也可以使用 createGlobalTempView 创建全局临时视图，全局临时视图可以在所有会话之间共享，并直到整个 Spark 应用程序终止后才会消失。
     * 1.3 全局临时视图被定义在内置的 global_temp 数据库下，需要使用限定名称进行引用，如 SELECT * FROM global_temp.view1
     */
    df.createOrReplaceTempView("emp")

    /*// 注册为全局临时视图
    df.createGlobalTempView("gemp")

    // 使用限定名称进行引用
    spark.sql("SELECT ename,depno FROM global_temp.gemp").show()*/


    // 2.查询指定字段：员工姓名及性别
    spark.sql("SELECT ename , gender FROM emp").show()

    // 3.查询工资大于2000 的员工信息
    spark.sql("SELECT * FROM emp WHERE sal > 2000").show()

    // 4.按照部门编号降序，工资升序进行查询
    spark.sql("SELECT * FROM emp ORDER BY depno DESC , sal ASC").show()

    // 5.查询工资最高的 3 名员工的信息
    spark.sql("SELECT * FROM emp ORDER BY sal DESC LIMIT 3").show()

    // 6.查询所有部门编号
    spark.sql("SELECT DISTINCT(depno) FROM emp").show()

    // 7.分组统计男女性别人数
    spark.sql("SELECT COUNT(*) AS nums , gender FROM emp GROUP BY gender").show()

    // 8.分组统计部门人数
    spark.sql("SELECT COUNT(1) AS nums , depno FROM emp GROUP BY depno").show()

    // 9.分组统计每个部门最高工资
    spark.sql("SELECT depno , MAX(sal) AS max_sal FROM emp GROUP BY depno").show()
  }
}

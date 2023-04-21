package com.wyk.spark.sql.spark_sql

import org.apache.spark.sql.SparkSession

/**
 * @ObjectName Aggregations
 * @Author wangyingkang
 * @Date 2022/4/14 09:34
 * @Version 1.0
 * @Description spark-sql常用聚合函数
 * */
object Aggregations {
  def main(args: Array[String]): Unit = {
    // 需要导入 spark sql 内置的函数包
    import org.apache.spark.sql.functions._

    val spark = SparkSession.builder().appName("aggregations").master("local[2]").getOrCreate()

    //数据准备
    val empDF = spark.read.format("json")
      .load("input/json/emp2.json")

    empDF.createOrReplaceTempView("emp")
    empDF.show()

    /**
     * 1. count 计算员工人数
     */
    empDF.select(count("ename")).show()
    //等价于
    spark.sql("SELECT COUNT(ename) FROM emp").show()

    /**
     * 2. countDistinct 计算姓名不重复的员工人数
     */
    empDF.select(countDistinct("ename")).show()
    //等价于
    spark.sql("SELECT DISTINCT( COUNT(ename) ) FROM emp").show()

    /**
     * 3.approx_count_distinct: 通常在使用大型数据集时，你可能关注的只是近似值而不是准确值，这时可以使用 approx_count_distinct 函数，并可以使用第二个参数指定最大允许误差。
     */
    empDF.select(approx_count_distinct("ename", 0.1)).show()

    /**
     * 4. first & last: 获取 DataFrame 中指定列的第一个值或者最后一个值。
     */
    empDF.select(first("ename"), last("job")).show()

    /**
     * 5.min & max: 获取 DataFrame 中指定列的最小值或者最大值。
     */
    empDF.select(min("sal"), max("sal")).show()

    /**
     * 6. sum & sumDistinct: 求和以及求指定列所有不相同的值的和。
     */
    empDF.select(sum("sal"), sumDistinct("sal")).show()
    //等价于
    spark.sql("SELECT SUM(sal) , SUM( DISTINCT(sal) ) FROM EMP").show()

    /**
     * 7. avg: 内置的求平均数的函数。
     */
    empDF.select(avg("sal")).show()
    //等价于
    spark.sql("SELECT AVG(sal) FROM emp").show()

    /**
     * 8. 数学函数: Spark SQL 中还支持多种数学聚合函数，用于通常的数学计算，以下是一些常用的例子：
     */
    // 8.1 计算总体方差、均方差、总体标准差、样本标准差
    empDF.select(var_pop("sal"), var_samp("sal"), stddev_pop("sal"), stddev_samp("sal")).show()

    // 8.2 计算偏度和峰度
    empDF.select(skewness("sal"), kurtosis("sal")).show()

    // 8.3 计算两列的皮尔逊相关系数、样本协方差、总体协方差。(这里只是演示，员工编号和薪资两列实际上并没有什么关联关系)
    empDF.select(corr("empno", "sal"), covar_samp("empno", "sal"), covar_pop("empno", "sal")).show()

    /**
     * 9. 聚合数据到集合
     */
    empDF.select(collect_set("job"), collect_list("ename")).show()

    /**
     * 10. 分组聚合
     */
    //10.1 计算每个部的职位数
    empDF.groupBy("deptno", "job").count().show()
    //等价于
    spark.sql("SELECT deptno,job,COUNT(1) FROM emp GROUP BY deptno , job").show()

    //10.2 计算每个部门员工人数和薪资总数
    empDF.groupBy("deptno").agg(count("ename").alias("人数"), sum("sal").alias("总工资")).show()
    //等价语法
    empDF.groupBy("deptno").agg("ename" -> "count", "sal" -> "sum").show()
    //等价sql
    spark.sql("SELECT deptno , COUNT(ename) , SUM(sal) FROM emp GROUP BY deptno").show()

  }
}

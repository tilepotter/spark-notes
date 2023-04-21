package com.wyk.spark.sql.spark_sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @ObjectName Join
 * @Author wangyingkang
 * @Date 2022/4/14 10:28
 * @Version 1.0
 * @Description Spark-SQL 常用JOIN操作
 *              Spark 中支持多种连接类型：
 *              Inner Join : 内连接；
 *              Full Outer Join : 全外连接；
 *              Left Outer Join : 左外连接；
 *              Right Outer Join : 右外连接；
 *              Left Semi Join : 左半连接；
 *              Left Anti Join : 左反连接；
 *              Natural Join : 自然连接；
 *              Cross (or Cartesian) Join : 交叉 (或笛卡尔) 连接。
 *              左半连接和左反连接，这两个连接等价于关系型数据库中的 IN 和 NOT IN
 * */
object Join {
  def main(args: Array[String]): Unit = {
    // 需要导入 spark sql 内置的函数包
    import org.apache.spark.sql.functions._

    val spark = SparkSession.builder().appName("Spark-SQL").master("local[2]").getOrCreate()

    //数据准备
    val empDF: DataFrame = spark.read.format("json")
      .load("input/json/emp2.json")
    empDF.createOrReplaceTempView("emp")

    val deptDF: DataFrame = spark.read.format("json")
      .load("input/json/dept2.json")
    deptDF.createOrReplaceTempView("dept")

    //empDF.show()
    //deptDF.show()

    /**
     * 1. INNER JOIN
     */
    // 定义连接表达式
    val joinExpression = empDF.col("deptno") === deptDF.col("deptno")
    // 连接查询
    empDF.join(deptDF, joinExpression, joinType = "inner").select("ename", "dname").show()
    // 等价sql
    spark.sql("SELECT ename,dname FROM emp INNER JOIN dept ON emp.deptno=dept.deptno").show()

    /**
     * 2. FULL OUTER JOIN
     */
    empDF.join(deptDF, joinExpression, "outer").show()
    // 等价sql
    spark.sql("SELECT * FROM emp FULL OUTER JOIN dept ON emp.deptno=dept.deptno").show()

    /**
     * 3. LEFT OUTER JOIN
     */
    empDF.join(deptDF, joinExpression, "left_outer").show()
    //等价sql
    spark.sql("SELECT * FROM emp LEFT OUTER JOIN dept ON emp.deptno=dept.deptno").show()

    /**
     * 4. RIGHT OUTER JOIN
     */
    empDF.join(deptDF, joinExpression, "right_outer").show()
    spark.sql("SELECT * FROM emp RIGHT OUTER JOIN dept ON emp.deptno=dept.deptno").show()

    /**
     * 5. LEFT SEMI JOIN 左半连接
     */
    empDF.join(deptDF, joinExpression, "left_semi").show()
    //等价于sql
    spark.sql("SELECT * FROM emp LEFT SEMI JOIN dept ON emp.deptno = dept.deptno").show()
    //等价于如下IN 语句
    spark.sql("SELECT * FROM emp WHERE deptno IN (SELECT deptno FROM dept)").show()

    /**
     * 6. LEFT ANTI JOIN 左反连接
     */
    empDF.join(deptDF, joinExpression, "left_anti").show()
    spark.sql("SELECT * FROM emp LEFT ANTI JOIN dept ON emp.deptno = dept.deptno").show()
    spark.sql("SELECT * FROM emp WHERE deptno NOT IN ( SELECT deptno FROM dept )").show()

    /**
     * 7. CROSS JOIN 笛卡尔积
     */
    empDF.join(deptDF, joinExpression, "cross").show()
    spark.sql("SELECT * FROM emp CROSS JOIN dept ON emp.deptno = dept.deptno").show()

    /**
     * 8.NATURAL JOIN: 自然连接是在两张表中寻找那些数据类型和列名都相同的字段，然后自动地将他们连接起来，并返回所有符合条件的结果。
     * 由于自然连接常常会产生不可预期的结果，所以并不推荐使用。
     */
    spark.sql("SELECT * FROM emp NATURAL JOIN dept ").show()
    //程序自动推断出使用两张表都存在的 dept 列进行连接，其实际等价于：
    spark.sql("SELECT * FROM emp JOIN dept ON emp.deptno = dept.deptno").show()
  }
}

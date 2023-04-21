package com.wyk.spark.sql.external_datasource

import org.apache.spark.sql.SparkSession

import java.util.Properties

/**
 * @ObjectName Jdbc
 * @Author wangyingkang
 * @Date 2022/4/13 16:24
 * @Version 1.0
 * @Description Spark 同样支持与传统的关系型数据库进行数据读写,本例使用spark-sql读写mysql
 * */
object Jdbc {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark-SQL").master("local[2]").getOrCreate()

    /**
     * 1.1 读取全表数据示例
     */
    spark.read
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://127.0.0.1:3306/test")
      .option("user", "root")
      .option("password", "admin")
      .option("dbtable", "area_dic")
      .load().show(10)

    /**
     * 1.2 从查询结果读取数据
     */
    val querySql = "(select * from area_dic where layer=1) as area_dic"
    spark.read
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://127.0.0.1:3306/test")
      .option("user", "root")
      .option("password", "admin")
      .option("dbtable", querySql)
      .option("numPartitions", 10) //读取数据的并行度
      .load().show()

    /**
     * 1.3 也可以使用如下的写法进行数据的过滤：
     */
    val props = new Properties()
    props.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    props.setProperty("user", "root")
    props.setProperty("password", "admin")
    val predicates = Array(" parent_code='530000' and layer='2'") //指定谓词数据查询条件
    spark.read.jdbc("jdbc:mysql://127.0.0.1:3306/test", "area_dic", predicates, props)
      .show()

    /**
     * 2 写入数据到mysql
     */
    val dfData = spark.read.format("json").option("multiline", "true")
      .load("input/json/emp.json")
      .toDF()

    dfData.write.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://127.0.0.1:3306/test")
      .option("user", "root")
      .option("password", "admin")
      .option("dbtable", "emp")
      .save()
  }
}

package com.wyk.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName FilterTest
 * @Author wangyingkang
 * @Date 2022/4/20 15:04
 * @Version 1.0
 * @Description filter算子测试：从服务器日志数据apache.log中获取2015年5月17日的请求路径
 * */
object FilterTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    val sc = new SparkContext(sparkConf)

    val fileRDD = sc.textFile("input/data/apache.log")

    //拆分每一行数据，过滤17/05/2015请求的日志
    val filterRDD = fileRDD.filter(
      line => {
        val data = line.split(" ")
        val str = data(3)
        str.startsWith("17/05/2015")
      }
    )

    filterRDD.collect().foreach(println)

    sc.stop()
  }
}

package com.wyk.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

object Filter {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    val sc = new SparkContext(sparkConf)

    val list = List(3, 6, 9, 10, 11, 12, 17, 19)


    val rdd = sc.parallelize(list)

    //过滤输出list中大于等于10的元素
    //rdd.filter(_ <= 10).collect().foreach(println)

    //过滤输出list中的奇数
    rdd.filter(num => num % 2 == 0).collect().foreach(println)

    sc.stop()
  }

}
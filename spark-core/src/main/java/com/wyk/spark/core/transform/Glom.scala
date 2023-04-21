package com.wyk.spark.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName Glom
 * @Author wangyingkang
 * @Date 2022/4/20 10:51
 * @Version 1.0
 * @Description 将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变
 * */
object Glom {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("MapPartitions-Test")

    val sc = new SparkContext(sparkConf)

    val rdd = sc.parallelize(List(1, 2, 3, 4), 2)

    // List => Int
    // Int => Array
    val glomRDD: RDD[Array[Int]] = rdd.glom()

    glomRDD.collect().foreach(data => println(data.mkString("Array(", ", ", ")")))

    sc.stop()
  }
}

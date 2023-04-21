package com.wyk.spark.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName GlomTest
 * @Author wangyingkang
 * @Date 2022/4/20 11:03
 * @Version 1.0
 * @Description 计算所有分区最大值求和(分区内取最大值，分区间最大值求和)
 * */
object GlomTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("MapPartitions-Test")

    val sc = new SparkContext(sparkConf)

    val rdd = sc.parallelize(List(1, 2, 3, 4), 2)

    //将每个分区转换为int数组
    val glomRDD: RDD[Array[Int]] = rdd.glom()

    //求每个分区数组最大值
    //【1，2】、【3，4】
    //【2】、【4】
    //【6】
    val mapRDD: RDD[Int] = glomRDD.map(
      data => {
        data.max
      })

    println(mapRDD.collect().sum)

  }
}

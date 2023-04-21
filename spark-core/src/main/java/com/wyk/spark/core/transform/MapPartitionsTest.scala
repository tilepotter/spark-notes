package com.wyk.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName MapPartitionsTest
 * @Author wangyingkang
 * @Date 2022/4/20 10:29
 * @Version 1.0
 * @Description MapPartitions算子练习：获取每个数据分区的最大值
 * */
object MapPartitionsTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("MapPartitions-Test")

    val sc = new SparkContext(sparkConf)

    val list = List(1, 2, 3, 4)

    //以list为数据源，创建两分区的rdd
    val rdd = sc.makeRDD(list, 2)

    //使用mapPartitions 遍历每个分区数据，获取每个分区最大值
    //【1，2】、【3，4】
    //【2】、【4】
    val mapRDD = rdd.mapPartitions(
      iter => {
        List(iter.max).toIterator
      }
    )

    mapRDD.collect().foreach(println(_))

    sc.stop()
  }
}

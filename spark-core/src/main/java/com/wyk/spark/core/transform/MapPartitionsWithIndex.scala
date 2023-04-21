package com.wyk.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName MapPartitionsWithIndex
 * @Author wangyingkang
 * @Date 2022/4/20 10:27
 * @Version 1.0
 * @Description mapPartitionsWithIndex使用：
 *              与 mapPartitions 类似，但 func 类型为 (Int, Iterator<T>) => Iterator<U> ，其中第一个参数为分区索引。
 * */
object MapPartitionsWithIndex {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("MapPartitions-Test")

    val sc = new SparkContext(sparkConf)

    val list = List(1, 2, 3, 4)

    val rdd = sc.makeRDD(list, 2)

    //返回第一个分区的数据
    // 【1，2】、【3，4】
    // 【1，2】
    val mapRDD = rdd.mapPartitionsWithIndex(
      (index, iter) => {
        if (index == 0) {
          iter
        } else {
          Nil.iterator
        }
      }
    )

    mapRDD.collect().foreach(println)

    sc.stop()
  }
}

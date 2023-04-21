package com.wyk.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName MapPartitionsWithIndexTest
 * @Author wangyingkang
 * @Date 2022/4/20 10:43
 * @Version 1.0
 * @Description MapPartitionsWithIndex算子练习：以list为数据源构建rdd，以该rdd中每个数据值的分区号和数据值组成二元组返回，如（0，1），（1，3）
 * */
object MapPartitionsWithIndexTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("MapPartitions-Test")

    val sc = new SparkContext(sparkConf)

    val list = List(1, 2, 3, 4, 5)

    val rdd = sc.makeRDD(list, 2)

    //遍历分区数据，构建（分区号，数据值）二元组
    rdd.mapPartitionsWithIndex(
      (index, iter) => {
        iter.map(data => (index, data))
      }
    ).collect().foreach(println(_))

    sc.stop()
  }
}

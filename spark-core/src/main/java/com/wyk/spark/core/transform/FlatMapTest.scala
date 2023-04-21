package com.wyk.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName FlatMapTest
 * @Author wangyingkang
 * @Date 2022/4/20 10:57
 * @Version 1.0
 * @Description flatMap算子练习：将 List(List(1,2),3,List(4,5))进行扁平化操作
 * */
object FlatMapTest {
  def main(args: Array[String]): Unit = {
    // 创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.parallelize(List(List(1, 2), 3, List(4, 5)), 2)

    //类型匹配
    val flatMapRDD = rdd.flatMap(
      data => {
        data match {
          case list: List[Int] => list
          case _ => List(data)
        }
      })

    flatMapRDD.collect().foreach(println)

    sc.stop()
  }
}

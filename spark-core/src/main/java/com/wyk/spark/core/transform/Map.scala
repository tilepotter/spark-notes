package com.wyk.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

object Map {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val list = List(1, 2, 3, 4, 5)

    sc.parallelize(list).map(_ * 10).foreach(println)
  }

}

package com.wyk.spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName Foreach
 * @Author wangyingkang
 * @Date 2022/5/19 17:23
 * @Version 1.0
 * @Description
 * */
object Foreach {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    //收集以后在driver端遍历打印rdd
    rdd.collect().foreach(println)

    println("***********************")

    //分布式打印：在executor端执行
    rdd.foreach(println)

    sc.stop()
  }
}

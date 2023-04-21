package com.wyk.spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName CountByKey
 * @Author wangyingkang
 * @Date 2022/4/12 10:59
 * @Version 1.0
 * @Description 计算每个键出现的次数
 * */
object CountByKey {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val list = List(("hadoop", 10), ("hadoop", 12), ("flink", 10), ("flink", 1), ("spark", 2))
    sc.parallelize(list).countByKey().toList.foreach(println(_))
  }
}

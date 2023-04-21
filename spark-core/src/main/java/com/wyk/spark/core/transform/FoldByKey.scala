package com.wyk.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName FoldByKey
 * @Author wangyingkang
 * @Date 2022/4/25 17:11
 * @Version 1.0
 * @Description 当分区内计算规则和分区间计算规则相同时，aggregateByKey 就可以简化为 foldByKey
 * */
object FoldByKey {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)), 2)

    //分区内和分区间计算规则相同
    rdd.aggregateByKey(0)(_ + _, _ + _).collect().foreach(println)

    // 如果聚合计算时，分区内和分区间计算规则相同，spark提供了简化的方法：foldByKey()
    rdd.foldByKey(0)(_ + _).collect().foreach(println)

    sc.stop()
  }
}

package com.wyk.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName Cartesian
 * @Author wangyingkang
 * @Date 2022/4/11 17:52
 * @Version 1.0
 * @Description 计算笛卡尔积
 * */
object Cartesian {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val list1 = List("A", "B", "C")
    val list2 = List(1, 2, 3)

    sc.parallelize(list1).cartesian(sc.parallelize(list2)).foreach(println(_))
  }
}

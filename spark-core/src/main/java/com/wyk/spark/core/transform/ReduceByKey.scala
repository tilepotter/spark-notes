package com.wyk.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName ReduceByKey
 * @Author wangyingkang
 * @Date 2022/4/11 17:26
 * @Version 1.0
 * @Description reduceByKey : 相同的key的数据进行value数据的聚合操作
 *              scala语言中一般的聚合操作都是两两聚合，spark基于scala开发的，所以它的聚合也是两两聚合
 * */
object ReduceByKey {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("hadoop", 2), ("Spark", 3), ("Flink", 5), ("hadoop", 1), ("Spark", 5)))

    val reduceByKeyRDD = rdd.reduceByKey((x: Int, y: Int) => {
      println(s"x = ${x}, y = ${y}")
      x + y
    })

    println(reduceByKeyRDD.collect().mkString("Array(", ", ", ")"))

    sc.stop()
  }
}

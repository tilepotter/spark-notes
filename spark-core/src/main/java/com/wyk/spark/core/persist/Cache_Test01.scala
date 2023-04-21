package com.wyk.spark.core.persist

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName Cache
 * @Author wangyingkang
 * @Date 2022/5/23 14:29
 * @Version 1.0
 * @Description
 * */
object Cache_Test01 {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val list = List("Hello Scala", "Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatRDD.map(word => {
      println("@@@@@@@@@@@@")
      (word, 1)
    })

    val reduceRDD = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    println("**************************************")

    //调用mapRDD时候依据血缘关系又重新计算了一遍，并没有使用reduceRDD计算时已经计算出的mapRDD
    val groupRDD = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)

    sc.stop()
  }
}

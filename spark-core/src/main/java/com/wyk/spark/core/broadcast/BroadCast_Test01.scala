package com.wyk.spark.core.broadcast

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @ObjectName BroadCast_Test01
 * @Author wangyingkang
 * @Date 2022/5/24 16:24
 * @Version 1.0
 * @Description
 * */
object BroadCast_Test01 {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparConf)

    val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)), 3)

    //val rdd2 = sc.makeRDD(List(("a", 4), ("b", 5), ("c", 6)), 3)

    val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))

    //val joinRDD = rdd1.join(rdd2)
    //joinRDD.collect().foreach(println)

    //每个task任务里面都会存一份map
    rdd1.map {
      case (w, c) => {
        val l = map.getOrElse(w, 0)
        (w, (c, l))
      }
    }.collect().foreach(println)

    sc.stop()
  }
}

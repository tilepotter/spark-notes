package com.wyk.spark.core.broadcast

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @ObjectName BroadCast_Test01
 * @Author wangyingkang
 * @Date 2022/5/24 16:24
 * @Version 1.0
 * @Description 广播变量：分布式共享只读变量
 * */
object BroadCast_Test02 {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparConf)

    val rdd1 = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)), 3)

    var map = mutable.Map(("a", 4), ("b", 5), ("c", 6))

    //封装广播变量
    val broadCast: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

    //broadCast为共享变量，整个executor的jvm中只有一份
    rdd1.map {
      case (w, c) => {
        // 方法广播变量
        val l = broadCast.value.getOrElse(w, 0)
        (w, (c, l))
      }
    }.collect().foreach(println)

    sc.stop()
  }
}

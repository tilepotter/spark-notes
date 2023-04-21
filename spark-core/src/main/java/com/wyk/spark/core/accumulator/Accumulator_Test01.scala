package com.wyk.spark.core.accumulator

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName Accumulator_Test01
 * @Author wangyingkang
 * @Date 2022/5/24 15:40
 * @Version 1.0
 * @Description
 * */
object Accumulator_Test01 {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    // reduce : 分区内计算，分区间计算
    //val i = rdd.reduce(_ + _)
    //println(i)
    var sum = 0
    rdd.foreach(
      num => {
        sum += num
      }
    )

    //driver端会将 sum=0 闭包传递到executor端:【1，2】【sum=0】=> 【3】  【3，4】【sum=0】 =>【7】，但是最后两个分区计算的和不能返回driver端，所以driver端输出sum=0
    println("sum = " + sum)

    sc.stop()
  }
}

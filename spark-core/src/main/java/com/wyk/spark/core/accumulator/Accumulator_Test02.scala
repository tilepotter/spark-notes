package com.wyk.spark.core.accumulator

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName Accumulator_Test01
 * @Author wangyingkang
 * @Date 2022/5/24 15:40
 * @Version 1.0
 * @Description 累加器：分布式共享只写变量
 *              累加器用来把 Executor 端变量信息聚合到 Driver 端。
 *              在 Driver 程序中定义的变量，在 Executor 端的每个 Task 都会得到这个变量的一份新的副本，
 *              每个 task 更新这些副本的值后， 传回 Driver 端进行 merge。
 * */
object Accumulator_Test02 {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("Acc")
    val sc = new SparkContext(sparConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    //spark默认提供了简单数据聚合的累加器
    //获取上下文环境的累加器
    val acc = sc.longAccumulator("acc")

    rdd.foreach(
      num => {
        //使用累加器
        acc.add(num)
      }
    )

    //获取累加器的值
    println(acc.value)

    sc.stop()
  }
}

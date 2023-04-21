package com.wyk.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName OuterJoin
 * @Author wangyingkang
 * @Date 2022/4/25 17:46
 * @Version 1.0
 * @Description LeftOuterJoin:类似于 SQL 语句的左外连接
 *              RightOuterJoin:类似于 SQL 语句的右外连接
 * */
object OuterJoin {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("b", 2) //, ("c", 3)
    ))

    val rdd2 = sc.makeRDD(List(
      ("a", 4), ("b", 5), ("c", 6)
    ))
    val leftJoinRDD = rdd1.leftOuterJoin(rdd2)

    leftJoinRDD.collect().foreach(println)

    //val rightJoinRDD = rdd1.rightOuterJoin(rdd2)

    //rightJoinRDD.collect().foreach(println)


    sc.stop()

  }
}

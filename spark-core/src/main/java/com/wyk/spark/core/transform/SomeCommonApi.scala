package com.wyk.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName SomeCommonApi
 * @Author wangyingkang
 * @Date 2022/4/11 17:00
 * @Version 1.0
 * @Description 其他一些常见的transform API:
 *              1.抽样 sample
 *              2.合并 union
 *              3.交集 intersection
 *              4.差集 subtract
 *              5.去重 distinct
 *              6.拉链 zip
 * */
object SomeCommonApi {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    /**
     * 1.抽样 sample
     * 数据采样。有三个可选参数：设置是否放回 (withReplacement)、采样的百分比 (fraction)、随机数生成器的种子 (seed)
     * // 第一个参数:抽取的数据是否放回，true:放回，false:不放回
     * // 第二个参数:抽取的几率，范围在[0,1]之间,0:全不取;1:全取;表示每一个元素被期望抽取到的次数
     * // 第三个参数:随机数种子
     */
    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 1)
    println("-------------------- sample() 算子： --------------------")
    println(rdd.sample(withReplacement = false, 0.5).collect().mkString("Array(", ", ", ")"))

    val rdd1 = sc.makeRDD(List(1, 3, 5), 1)
    val rdd2 = sc.makeRDD(List(2, 4, 5), 1)

    /**
     * 2.union
     * 合并两个 RDD
     */
    println("-------------------- union() 算子： --------------------")
    println(rdd1.union(rdd2).collect().mkString("Array(", ", ", ")"))


    /**
     * 3.intersection
     * 求两个 RDD 的交集
     */
    println("-------------------- intersection() 算子： --------------------")
    println(rdd1.intersection(rdd2).collect().mkString("Array(", ", ", ")"))

    /**
     * 4. subtract 求两个 RDD 的差集
     */
    println("-------------------- subtract() 算子： --------------------")
    println(rdd1.subtract(rdd2).collect().mkString("Array(", ", ", ")"))


    /**
     * 5.distinct
     * 去重
     */
    println("-------------------- distinct() 算子： --------------------")
    val rdd3 = sc.makeRDD(List(1, 2, 2, 3, 3, 4, 5), 1)
    println(rdd3.distinct().collect().mkString("Array(", ", ", ")"))

    /**
     * 6. zip 拉链  1-2,3-4,5-5
     */
    println("-------------------- zip() 算子： --------------------")
    rdd1.zip(rdd2).foreach(println)

  }
}

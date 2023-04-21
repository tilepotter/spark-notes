package com.wyk.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName Repartition
 * @Author wangyingkang
 * @Date 2022/4/20 17:35
 * @Version 1.0
 * @Description 函数说明:
 *              该操作内部其实执行的是 coalesce 操作，参数 shuffle 的默认值为 true。
 *              无论是将分区数多的 RDD 转换为分区数少的 RDD，还是将分区数少的 RDD 转换为分区数多的 RDD，
 *              repartition 操作都可以完成，因为无论如何都会经 shuffle 过程。
 * */
object Repartition {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    /**
     * coalesce算子可以扩大分区的，但是如果不进行shuffle操作，是没有意义，不起作用。
     * 所以如果想要实现扩大分区的效果，需要使用shuffle操作
     * spark提供了一个简化的操作:
     * 缩减分区：coalesce，如果想要数据均衡，可以采用shuffle
     * 扩大分区：repartition, 底层代码调用的就是coalesce，而且肯定采用shuffle
     */
    //【1，2，3】，【4，5，6】 => 【1，2】，【3，4】，【5，6】 分区减少，分区数据进行shuffle
    //val newRDD = rdd.coalesce(3, shuffle = true)

    //底层代码调用的就是coalesce，而且肯定采用shuffle
    val newRDD = rdd.repartition(3)

    newRDD.saveAsTextFile("output/repartition")

    sc.stop()
  }
}

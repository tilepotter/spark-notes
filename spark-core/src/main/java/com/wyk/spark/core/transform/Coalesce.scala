package com.wyk.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName Coalesce
 * @Author wangyingkang
 * @Date 2022/4/20 17:26
 * @Version 1.0
 * @Description 函数说明:根据数据量缩减分区，用于大数据集过滤后，提高小数据集的执行效率
 *              当 spark 程序中，存在过多的小任务的时候，可以通过 coalesce 方法，收缩合并分区，减少分区的个数，减小任务调度成本
 * */
object Coalesce {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    // coalesce方法默认情况下不会将分区的数据打乱重新组合
    // 这种情况下的缩减分区可能会导致数据不均衡，出现数据倾斜
    // 如果想要让数据均衡，可以进行shuffle处理：第二个参数传true，使用shuffle处理
    //【1，2】，【3，4】，【5，6】 => 【1，2】，【3，4，5，6】 默认分区减少，但是分区数据没有进行重分配
    //val newRDD = rdd.coalesce(2)

    //【1，2】，【3，4】，【5，6】 => 【1，4，5】，【2，3，6】 分区减少，分区数据进行shuffle
    val newRDD = rdd.coalesce(2, shuffle = true)

    newRDD.saveAsTextFile("output/coalesce/")

    sc.stop()
  }
}

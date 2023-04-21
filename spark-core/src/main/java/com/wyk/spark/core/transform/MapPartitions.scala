package com.wyk.spark.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * mapPartitions使用
 */
object MapPartitions {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val list = List(1, 2, 3, 4)

    /**
     * 1.mapPartitions:
     * 与 map 类似，但函数单独在 RDD 的每个分区上运行， func函数的类型为 Iterator<T> => Iterator<U> (其中 T 是 RDD 的类型)，即输入和输出都必须是可迭代类型。
     * 可以以分区为单位进行数据转换操作
     * 但是会将整个分区的数据加载到内存进行引用
     * 如果处理完的数据是不会被释放掉，存在对象的引用。
     * 在内存较小，数据量较大的场合下，容易出现内存溢出。
     */
    val rdd = sc.parallelize(list, 2)

    //可查看输出到文件，共两个分区
    rdd.saveAsTextFile("output/map")

    val mapRDD: RDD[Int] = rdd.mapPartitions(
      iter => {
        println(">>>>>>>>>") //两个分区，共打印两次
        iter.map(_ * 2)
      })

    mapRDD.collect().foreach(println)

    sc.stop()
  }
}

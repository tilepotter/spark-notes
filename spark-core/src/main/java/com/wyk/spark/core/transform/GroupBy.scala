package com.wyk.spark.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName GroupBy
 * @Author wangyingkang
 * @Date 2022/4/20 15:11
 * @Version 1.0
 * @Description 将数据根据指定的规则进行分组, 分区默认不变，但是数据会被打乱重新组合，我们将这样的操作称之为 shuffle。
 *              极限情况下，数据可能被分在同一个分区中
 * */
object GroupBy {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List("Hadoop", "Spark", "Hive", "Storm"), 2)

    //按照元素首字母分组：首字母相同的会被分在同一个组里面
    val groupRDD: RDD[(Char, Iterable[String])] = rdd.groupBy(_.charAt(0))

    groupRDD.collect().foreach(println)

    sc.stop()
  }
}

package com.wyk.spark.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName GroupByKey
 * @Author wangyingkang
 * @Date 2022/4/11 17:11
 * @Version 1.0
 * @Description 按照键进行分组: 将数据源中的数据，相同key的数据分在一个组中，形成一个对偶元组
 * */
object GroupByKey {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(("hadoop", 2), ("Spark", 3), ("Flink", 5), ("hadoop", 1), ("Spark", 5)))

    // groupByKey : 将数据源中的数据，相同key的数据分在一个组中，形成一个对偶元组
    //              元组中的第一个元素就是key，
    //              元组中的第二个元素就是相同key的value的集合
    val newRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()

    newRDD.collect().foreach(println)

    //groupBy()
    val groupByRDD: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)

    groupByRDD.collect().foreach(println)

    sc.stop()

  }

}

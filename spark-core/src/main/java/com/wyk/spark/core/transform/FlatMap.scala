package com.wyk.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * flatMap(func) 与 map 类似，但每一个输入的 item 会被映射成 0 个或多个输出的 items（ func 返回类型需要为 Seq）
 */
object FlatMap {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    //val list = List(List(1, 2), List(3), List(), List(4, 5))

    //sc.parallelize(list,2).flatMap(_.toList).map(_ * 10).foreach(println)

    /**
     * flatMap 这个算子在日志分析中使用概率非常高，这里进行一下演示：
     * 拆分输入的每行数据为单个单词，并赋值为 1，代表出现一次，之后按照单词分组并统计其出现总次数，
     * 代码如下：
     */
    //    val lines = List("flink spark hive", "flink flume hadoop", "spark flink flume")
    //    val linesRDD = sc.parallelize(lines, 1)
    //    println(linesRDD.getNumPartitions)

    val fileRDD = sc.textFile("input/wordcount.txt")

    fileRDD.flatMap(lines => lines.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
      .foreach(println)

  }

}

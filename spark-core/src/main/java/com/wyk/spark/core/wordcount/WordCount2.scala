package com.wyk.spark.core.wordcount

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName WordCount2
 * @Author wangyingkang
 * @Date 2022/4/11 17:31
 * @Version 1.0
 * @Description
 * */
object WordCount2 {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("WordCount")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val fileRDD = sc.textFile("input/txt/wordcount.txt")

    fileRDD.flatMap(e => e.split(" ")).map(word => (word, 1))
      .reduceByKey((x, y) => (x + y))
      .foreach(println)
  }

}

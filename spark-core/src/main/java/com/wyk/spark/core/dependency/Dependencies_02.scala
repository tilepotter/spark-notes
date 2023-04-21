package com.wyk.spark.core.dependency

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName Dependencies
 * @Author wangyingkang
 * @Date 2022/5/19 17:49
 * @Version 1.0
 * @Description RDD 依赖关系: 两个相邻 RDD 之间的关系
 *              RDD 窄依赖: 窄依赖表示每一个父(上游)RDD 的 Partition 最多被子(下游)RDD 的一个 Partition 使用，窄依赖我们形象的比喻为独生子女。
 *              RDD 宽依赖: 宽依赖表示同一个父(上游)RDD 的 Partition 被多个子(下游)RDD 的 Partition 依赖，会 引起 Shuffle，总结:宽依赖我们形象的比喻为多生
 * */
object Dependencies_02 {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    //读取文件数据
    val fileRDD: RDD[String] = sc.textFile("input/txt/wordcount.txt")
    println(fileRDD.dependencies)
    println("***********************")

    //将文件中的数据进行分词
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    println(wordRDD.dependencies)
    println("***********************")

    //转换数据结构 word => (word,1)
    val word2OneRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    println(word2OneRDD.dependencies)
    println("***********************")


    // 将转换结构后的数据按照相同的单词进行分组聚合
    val word2CountRDD: RDD[(String, Int)] = word2OneRDD.reduceByKey(_ + _)
    println(word2CountRDD.dependencies)
    println("***********************")

    // 将数据聚合结果采集到内存中
    val word2Count: Array[(String, Int)] = word2CountRDD.collect()

    //打印结果
    word2Count.foreach(println)

    //关闭spark连接
    sc.stop()
  }
}

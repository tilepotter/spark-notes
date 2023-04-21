package com.wyk.spark.core.dependency

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @ObjectName Dependencies
 * @Author wangyingkang
 * @Date 2022/5/19 17:49
 * @Version 1.0
 * @Description RDD 血缘关系:
 *              RDD 只支持粗粒度转换，即在大量记录上执行的单个操作。将创建 RDD 的一系列 Lineage (血统)记录下来，以便恢复丢失的分区。RDD 的 Lineage 会记录 RDD 的元数据信息和转 换行为，当该 RDD 的部分分区数据丢失时，它可以根据这些信息来重新运算和恢复丢失的 数据分区。
 * */
object Dependencies_01 {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    //读取文件数据
    val fileRDD: RDD[String] = sc.textFile("input/txt/wordcount.txt")
    println(fileRDD.toDebugString)
    println("***********************")

    //将文件中的数据进行分词
    val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))
    println(wordRDD.toDebugString)
    println("***********************")

    //转换数据结构 word => (word,1)
    val word2OneRDD: RDD[(String, Int)] = wordRDD.map((_, 1))
    println(word2OneRDD.toDebugString)
    println("***********************")


    // 将转换结构后的数据按照相同的单词进行分组聚合
    val word2CountRDD: RDD[(String, Int)] = word2OneRDD.reduceByKey(_ + _)
    println(word2CountRDD.toDebugString)
    println("***********************")

    // 将数据聚合结果采集到内存中
    val word2Count: Array[(String, Int)] = word2CountRDD.collect()

    //打印结果
    word2Count.foreach(println)

    //关闭spark连接
    sc.stop()
  }
}

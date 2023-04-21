package com.wyk.spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName SaveAsTextFile
 * @Author wangyingkang
 * @Date 2022/4/12 11:02
 * @Version 1.0
 * @Description 将 dataset 中的元素以文本文件的形式写入本地文件系统、HDFS 或其它 Hadoop 支持的文件系统中。
 *              Spark 将对每个元素调用 toString 方法，将元素转换为文本文件中的一行记录。
 * */
object SaveAsTextFile {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val list = List(("hadoop", 10), ("hadoop", 10), ("storm", 3), ("storm", 3), ("azkaban", 1))
    sc.parallelize(list).saveAsTextFile("saveAsText")
  }
}

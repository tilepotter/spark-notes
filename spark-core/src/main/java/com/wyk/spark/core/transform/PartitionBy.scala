package com.wyk.spark.core.transform

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @ObjectName PartitionBy
 * @Author wangyingkang
 * @Date 2022/4/25 16:35
 * @Version 1.0
 * @Description 将数据按照指定 Partitioner 重新进行分区。Spark 默认的分区器是 HashPartitioner
 * */
object PartitionBy {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc")), 3)

    // partitionBy根据指定的分区规则对数据进行重分区
    val newRDD = rdd.partitionBy(new HashPartitioner(2))

    newRDD.saveAsTextFile("output/partitionBy")

    sc.stop()
  }
}

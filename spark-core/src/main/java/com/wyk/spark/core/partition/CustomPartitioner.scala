package com.wyk.spark.core.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

/**
 * @ObjectName CustomPartitions
 * @Author wangyingkang
 * @Date 2022/5/24 15:21
 * @Version 1.0
 * @Description 自定义分区器
 * */
object CustomPartitioner {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    val sc = new SparkContext(sparkConf)

    val list = List(("nba", "2010-06-07湖人战胜凯尔特人夺冠")
      , ("cba", "杜峰担任广东主教练")
      , ("nba", "2016-06-10骑士逆战勇士夺队史首冠")
      , ("ncaa", "1986-05-01 北卡乔丹被公牛用状元签选中"))

    val rdd: RDD[(String, String)] = sc.makeRDD(list, 3)

    val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner())

    partRDD.saveAsTextFile("output/customPartitioner/")

    sc.stop()
  }

  /**
   * 自定义分区器
   * 1. 继承Partitioner
   * 2. 重写方法
   */
  class MyPartitioner extends Partitioner {
    // 分区数量
    override def numPartitions: Int = 3

    // 根据数据的key值返回数据所在的分区索引（从0开始）
    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "ncaa" => 1
        case _ => 2
      }

    }
  }
}

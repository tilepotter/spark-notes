package com.wyk.spark.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat

/**
 * @ObjectName GroupByTest
 * @Author wangyingkang
 * @Date 2022/4/20 15:17
 * @Version 1.0
 * @Description groupBy算子测试：从服务器日志数据apache.log中获取每个时间段访问量。
 * */
object GroupByTest {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.textFile("input/data/apache.log")

    val timeRDD: RDD[(Int, Iterable[(Int, Int)])] = rdd.map(
      line => {
        val data = line.split(" ")
        val time = data(3)
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val date = sdf.parse(time)
        val hours = date.getHours
        (hours, 1)
      }
    ).groupBy(_._1)

    timeRDD.map {
      case (hour, iter) => {
        (hour, iter.size)
      }
    }.collect.foreach(println)

    sc.stop()
  }
}

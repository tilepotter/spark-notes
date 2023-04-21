package com.wyk.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName MapTest
 * @Author wangyingkang
 * @Date 2022/4/20 09:59
 * @Version 1.0
 * @Description map算子练习：从服务器日志数据apache.log中获取用户请求URL资源路径
 * */
object MapTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Map-Test")
    val sc = new SparkContext(sparkConf)

    //读取日志数据apache.log
    val fileRDD = sc.textFile("input/data/apache.log")

    //对读取的一行数据进行映射转换
    val result = fileRDD.map(lines => {
      val fields = lines.split(" ")
      fields(6)
    })

    result.collect().foreach(println(_))

    sc.stop()
  }
}

package com.wyk.spark.streaming.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ObjectName WordCount
 * @Author wangyingkang
 * @Date 2022/4/14 14:51
 * @Version 1.0
 * @Description 使用Spark-Streaming进行词频统计
 * */
object NetworkWordCountV1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("NetworkWordCountV1").setMaster("local[2]")
    //指定微批处理的时间间隔为5秒
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    //创建文本输入流,并进行词频统计
    val lines = streamingContext.socketTextStream("localhost", 9999)

    lines.flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .print()

    //启动服务
    streamingContext.start()
    //等待服务, 使服务处于等待和可用的状态
    streamingContext.awaitTermination()
  }
}

package com.wyk.spark.streaming.wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ObjectName NetworkWordCountV2
 * @Author wangyingkang
 * @Date 2022/4/14 15:08
 * @Version 1.0
 * @Description NetworkWordCountV1 的词频统计程序，只能统计每一次输入文本中单词出现的数量，想要统计所有历史输入中单词出现的数量，可以使用 updateStateByKey 算子
 * */
object NetworkWordCountV2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("NetworkWordCountV2").setMaster("local[2]")
    //指定微批处理的时间间隔为5秒
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    //必须设置检查点，这样当使用 updateStateByKey 算子时，它会去检查点中取出上一次保存的信息
    streamingContext.checkpoint("hdfs://localhost:8020/spark-streaming")

    val lines: ReceiverInputDStream[String] = streamingContext.socketTextStream("localhost", 9999)
    lines.flatMap(_.split(" "))
      .map(word => (word, 1))
      .updateStateByKey[Int](updateFunction _) //updateStateByKey
      .print()

    //启动服务
    streamingContext.start()
    //等待服务, 使服务处于等待和可用的状态
    streamingContext.awaitTermination()
  }

  /**
   * 累计求和
   *
   * @param currentValues 当前的数据
   * @param preValues     之前的数据
   * @return 相加后的数据
   */
  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)
    return Some(current + pre)
  }
}

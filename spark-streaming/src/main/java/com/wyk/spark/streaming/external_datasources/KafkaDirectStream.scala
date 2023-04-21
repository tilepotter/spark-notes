package com.wyk.spark.streaming.external_datasources

import com.wyk.spark.streaming.wordcount.NetworkWordCountV2.updateFunction
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ObjectName KafkaDirectStream
 * @Author wangyingkang
 * @Date 2022/4/14 16:22
 * @Version 1.0
 * @Description spark streaming 整合 kafka
 * */
object KafkaDirectStream {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("NetworkWordCountV2").setMaster("local[2]")
    //指定微批处理的时间间隔为5秒
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    //必须设置检查点，这样当使用 updateStateByKey 算子时，它会去检查点中取出上一次保存的信息
    streamingContext.checkpoint("hdfs://localhost:8020/spark-streaming")

    val params = Map[String, Object](
      /*
      * 指定 broker 的地址清单，清单里不需要包含所有的 broker 地址，生产者会从给定的 broker 里查找其他 broker 的信息。
      * 不过建议至少提供两个 broker 的信息作为容错。
      */
      "bootstrap.servers" -> "localhost:9092",
      //键的序列化器
      "key.deserializer" -> classOf[StringDeserializer],
      //值的序列化器
      "value.deserializer" -> classOf[StringDeserializer],
      //消费者所在分组的 ID
      "group.id" -> "spark-streaming-group",
      /*
       * 该属性指定了消费者在读取一个没有偏移量的分区或者偏移量无效的情况下该作何处理:
       * latest: 在偏移量无效的情况下，消费者将从最新的记录开始读取数据（在消费者启动之后生成的记录）
       * earliest: 在偏移量无效的情况下，消费者将从起始位置读取分区的记录
       */
      "auto.offset.reset" -> "latest",
      //是否自动提交offset
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    //可以同时订阅多个主题
    val topic = Array("test")
    val stream: DStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(streamingContext,
      //位置策略
      locationStrategy = PreferConsistent,
      //订阅主题
      Subscribe[String, String](topic, params)
    )
    val lines: DStream[String] = stream.map(record => record.value())
    val words = lines.flatMap(_.split(" "))
    val wordCount = words.map(word => (word, 1)).updateStateByKey(updateFunction _)
    wordCount.print()

    //启动服务
    streamingContext.start()
    //等待服务, 使服务处于等待和可用的状态
    streamingContext.awaitTermination()
  }
}

package com.wyk.spark.streaming.wordcount

import com.wyk.spark.streaming.util.JedisPoolUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

/**
 * @ObjectName NetworkWordCountToRedis
 * @Author wangyingkang
 * @Date 2022/4/14 15:29
 * @Version 1.0
 * @Description 使用 Redis 作为客户端，把每一次词频统计的结果写入到 Redis，并利用 Redis 的 HINCRBY 命令来进行词频统计
 * */
object NetworkWordCountToRedis {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("NetworkWordCountV2").setMaster("local[2]")
    //指定微批处理的时间间隔为5秒
    val streamingContext = new StreamingContext(sparkConf, Seconds(5))

    //创建文本输入流,并进行词频统计
    val lines = streamingContext.socketTextStream("localhost", 9999)

    val pairs: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)

    //保存数据到redis
    pairs.foreachRDD(
      rdd => rdd.foreachPartition {
        partitionRecords =>
          var jedis: Jedis = null
          try {
            jedis = JedisPoolUtil.getConnection
            partitionRecords.foreach(record => jedis.hincrBy("wordCount", record._1, record._2))
          } catch {
            case ex: Exception =>
              ex.printStackTrace()
          } finally {
            if (jedis != null)
              jedis.close()
          }
      }
    )

    //启动服务
    streamingContext.start()
    //等待服务, 使服务处于等待和可用的状态
    streamingContext.awaitTermination()
  }
}

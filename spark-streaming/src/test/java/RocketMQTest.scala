import com.alibaba.fastjson.JSON
import com.wyk.spark.streaming.external_datasources.RocketMQStreaming.Test
import com.wyk.spark.streaming.util.JedisPoolUtil
import org.apache.rocketmq.common.message.MessageExt
import org.apache.rocketmq.spark.{CanCommitOffsets, ConsumerStrategy, HasOffsetRanges, RocketMQConfig, RocketMqUtils}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import redis.clients.jedis.Jedis

import java.util

/**
 * @author wangyingkang
 * @date 2023/3/15 10:46
 * @version 1.0
 * @Description
 */
object RocketMQTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("RocketMQStreamingTest").setMaster("local[*]")
    //指定微批处理的时间间隔为5秒
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //如果启用 Spark 检查点，偏移量将存储在检查点中。这很容易启用，但也有缺点。你的输出操作必须是幂等的，因为你会得到重复的输出；交易不是一种选择。此外，如果您的应用程序代码已更改，您将无法从检查点恢复。对于计划升级，您可以通过同时运行新代码和旧代码来缓解这种情况（因为无论如何输出都需要幂等，它们不应该发生冲突）。但是对于需要更改代码的计划外故障，您将丢失数据，除非您有另一种方法来识别已知良好的起始偏移量。
    //ssc.checkpoint("hdfs://hdp02:8020/spark-streaming")

    // 配置RocketMQ消費者的參數
    val params: util.Map[String, String] = new util.HashMap[String, String]()
    params.put(RocketMQConfig.NAME_SERVER_ADDR, "hdp03:9876")
    params.put(RocketMQConfig.PULL_TIMEOUT_MS, "30000")

    val groupId = "test-consumer-group"
    val topic = "test-topic"

    // 使用RocketMQUtils創建一個流
    val dStream: InputDStream[MessageExt] = RocketMqUtils.createMQPullStream(ssc, groupId, topic, ConsumerStrategy.earliest, autoCommit = true, forceSpecial = false, failOnDataLoss = false, params)

    // 處理RocketMQ消息
    dStream.foreachRDD(
      (rdd: RDD[MessageExt]) => {
        //获取当前批次的RDD的偏移量
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd.foreach(msg => {
          println(new String(msg.getBody))
        })
        //提交当前批次的偏移量
        dStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}

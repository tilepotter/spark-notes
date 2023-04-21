package com.wyk.spark.core.persist

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName CheckPoint
 * @Author wangyingkang
 * @Date 2022/5/23 15:18
 * @Version 1.0
 * @Description 1、所谓的检查点（checkpoint）其实就是通过将 RDD 中间结果写入磁盘。
 *              由于血缘依赖过长会造成容错成本过高，这样就不如在中间阶段做检查点容错，如果检查点之后有节点出现问题，可以从检查点开始重做血缘，减少了开销。
 *              2、对 RDD 进行 checkpoint 操作并不会马上被执行，必须执行 Action 操作才能触发。
 *              3、缓存（cache）和检查点（checkpoint）的区别：
 *              3.1）Cache 缓存只是将数据保存起来，不切断血缘依赖。Checkpoint 检查点切断血缘依赖。
 *              3.2）Cache 缓存的数据通常存储在磁盘、内存等地方，可靠性低。Checkpoint 的数据通常存储在 HDFS 等容错、高可用的文件系统，可靠性高。
 *              3.2）建议对 checkpoint()的 RDD 使用 Cache 缓存，这样 checkpoint 的 job 只需从 Cache 缓存中读取数据即可，否则需要再从头计算一次 RDD。
 * */
object CheckPoint {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    // checkpoint 需要落盘，需要指定检查点保存路径
    sc.setCheckpointDir("./checkpoint")

    val list = List("Hello Scala", "Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatRDD.map(word => {
      println("@@@@@@@@@@@@")
      (word, 1)
    })

    // 增加缓存,避免再重新跑一个 job 做 checkpoint
    mapRDD.cache()

    // 检查点路径保存的文件，当作业执行完毕后，不会被删除
    // 一般保存路径都是在分布式存储系统：HDFS
    mapRDD.checkpoint()

    val reduceRDD = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)

    println("**************************************")

    val groupRDD = mapRDD.groupByKey()

    groupRDD.collect().foreach(println)

    sc.stop()
  }
}

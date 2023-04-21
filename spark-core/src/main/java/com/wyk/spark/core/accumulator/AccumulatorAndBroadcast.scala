package com.wyk.spark.core.accumulator

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName Test
 * @Author wangyingkang
 * @Date 2022/4/12 14:44
 * @Version 1.0
 * @Description
 * */
object AccumulatorAndBroadcast {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    /**
     * 1.accumulator 累加器
     */
    //定义累加器
    val accum = sc.longAccumulator("myAccum")
    val data = Array(1, 2, 3, 4)
    sc.parallelize(data).foreach(x => accum.add(x))
    println("累加器计算以后的值：" + accum.value)

    /**
     * 2.broadcast variable：广播变量
     */
    // 把一个数组定义为一个广播变量
    var broadcastVar = sc.broadcast(Array(1, 2, 3, 4, 5))

    println("广播变量使用：")
    sc.parallelize(broadcastVar.value).map(_ * 10).foreach(println(_))
  }
}

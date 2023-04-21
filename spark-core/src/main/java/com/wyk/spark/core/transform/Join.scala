package com.wyk.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName Join
 * @Author wangyingkang
 * @Date 2022/4/11 17:48
 * @Version 1.0
 * @Description // join : 两个不同数据源的数据，相同的key的value会连接在一起，形成元组
 *              在一个 (K, V) 和 (K, W) 类型的 Dataset 上调用时，返回一个 (K, (V, W)) 的 Dataset，等价于内连接操作。如果想要执行外连接，可以使用 leftOuterJoin, rightOuterJoin 和 fullOuterJoin 等算子。
 * */
object Join {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(List((1, "student01"), (24, "student02"), (3, "student03")))
    val rdd2 = sc.makeRDD(List((1, "teacher01"), (2, "teacher02"), (3, "teacher03")))

    // 如果两个数据源中key没有匹配上，那么数据不会出现在结果中
    // 如果两个数据源中key有多个相同的，会依次匹配，可能会出现笛卡尔乘积，数据量会几何性增长，会导致性能降低。
    rdd1.join(rdd2).foreach(println(_))

    sc.stop()
  }

}

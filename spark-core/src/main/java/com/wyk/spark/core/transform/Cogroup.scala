package com.wyk.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName Cogroup
 * @Author wangyingkang
 * @Date 2022/4/11 17:50
 * @Version 1.0
 * @Description 在一个 (K, V) 对的 Dataset 上调用时，返回多个类型为 (K, (Iterable<V>, Iterable<W>)) 的元组所组成的 Dataset。
 */
object Cogroup {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val list01 = List((1, "a"), (1, "a"), (2, "b"), (3, "e"))
    val list02 = List((1, "A"), (2, "B"), (3, "E"))
    val list03 = List((1, "[ab]"), (2, "[bB]"), (3, "eE"), (3, "eE"))

    sc.parallelize(list01).cogroup(sc.parallelize(list02), sc.parallelize(list03))
      .foreach(println(_))
  }

}

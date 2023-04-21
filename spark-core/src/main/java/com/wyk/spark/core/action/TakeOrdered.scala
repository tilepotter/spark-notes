package com.wyk.spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName TakeOrdered
 * @Author wangyingkang
 * @Date 2022/4/12 10:52
 * @Version 1.0
 * @Description 按自然顺序（natural order）或自定义比较器（custom comparator）排序后返回前 n 个元素。
 *              需要注意的是 takeOrdered 使用隐式参数进行隐式转换。
 *              所以在使用自定义排序时，需要继承 Ordering[T] 实现自定义比较器，然后将其作为隐式参数引入。
 * */
object TakeOrdered {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val list = List((1, "hadoop"), (1, "storm"), (1, "azkaban"), (1, "hive"))

    //  引入隐式默认值
    implicit val implicitOrdering: CustomOrdering = new CustomOrdering

    println(sc.parallelize(list).takeOrdered(5).mkString("Array(", ", ", ")"))
  }
}

// 继承 Ordering[T],实现自定义比较器，按照 value 值的长度进行排序
class CustomOrdering extends Ordering[(Int, String)] {
  override def compare(x: (Int, String), y: (Int, String)): Int =
    if (x._2.length > y._2.length) 1 else -1
}
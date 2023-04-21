package com.wyk.spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName SomeCommonApi
 * @Author wangyingkang
 * @Date 2022/4/12 10:39
 * @Version 1.0
 * @Description 常用action的API操作
 * */
object SomeCommonApi {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    /**
     * 1.reduce(func)	使用函数func执行归约操作
     */
    val list = List(1, 2, 3, 4, 5)
    println("reduce执行归约操作1：" + sc.parallelize(list).reduce((x, y) => x + y))
    println("reduce执行归约操作2：" + sc.parallelize(list).reduce(_ + _))

    /**
     * 2.collect()	以一个 array 数组的形式返回 dataset 的所有元素，适用于小结果集。
     */
    val fileRDD = sc.textFile("input/wordcount.txt")
    val result = fileRDD.collect()
    println("collect() => " + result.mkString("Array(", ", ", ")"))

    /**
     * 3.count()	返回 dataset 中元素的个数。
     */
    println("count()计算 list大小：" + sc.parallelize(list).count())

    /**
     * 4.first()	返回 dataset 中的第一个元素，等价于 take(1)。
     */
    println("first() 返回list中第一个元素：" + sc.parallelize(list).first())

    /**
     * 5.take(n)	将数据集中的前 n 个元素作为一个 array 数组返回。
     */
    println("take(n) 返回list前n个元素：" + sc.parallelize(list).take(3).mkString("Array(", ", ", ")"))

    /**
     * 6.takeSample(withReplacement, num, [seed])	对一个 dataset 进行随机抽样
     */
    println("takeSample() 对list进行随机抽样：" + sc.parallelize(list).takeSample(false, 2, 5).mkString("Array(", ", ", ")"))
  }

}

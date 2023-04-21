package com.wyk.spark.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName AggregateByKeyTest
 * @Author wangyingkang
 * @Date 2022/4/25 17:17
 * @Version 1.0
 * @Description AggregateByKey算子练习： 获取相同key的数据的平均值：key值之和/key出现的次数
 * */
object AggregateByKeyTest {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)), 2)

    //获取相同key的数据的平均值
    //【a,(3,2)】,【b,(3,1)】 || 【b,(9,2)】,【a,(6,1)】 ==> 【a,(9,3)】,【b,(12,3)】==> 【a,3】,【b,4】
    val newRDD: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )

    val resultRDD: RDD[(String, Int)] = newRDD.mapValues {
      case (num, cnt) =>
        num / cnt
    }

    resultRDD.collect().foreach(println)

    sc.stop()
  }
}

package com.wyk.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName AggregateByKey
 * @Author wangyingkang
 * @Date 2022/4/12 09:41
 * @Version 1.0
 * @Description 将数据根据不同的规则进行分区内计算和分区间计算
 *              当调用（K，V）对的数据集时，返回（K，U）对的数据集，其中使用给定的组合函数和 zeroValue 聚合每个键的值。与 groupByKey 类似，reduce 任务的数量可通过第二个参数 numPartitions 进行配置
 * */
object AggregateByKey {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)), 2)

    //取出每个分区内相同key的最大值然后分区间相加
    // aggregateByKey存在函数柯里化，有两个参数列表
    // 第一个参数列表,需要传递一个参数，表示为初始值
    //       主要用于当碰见第一个key的时候，和value进行分区内计算
    // 第二个参数列表需要传递2个参数
    //      第一个参数表示分区内计算规则
    //      第二个参数表示分区间计算规则
    //【a,2】,【b,3】      【b,5】,【a,6】  ==>  【a,8】,【b,8】
    rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    ).collect().foreach(println)

    //简化写法
    //【a,2】,【b,3】      【b,5】,【a,6】  ==>  【a,8】,【b,8】
    rdd.aggregateByKey(0)(math.max, _ + _).collect().foreach(println)

    //改变初始值为5，分区内和分区间计算逻辑相同：相同key的value相加
    //【a,8】,【b,8】     【b,14】【a,11】  ==>  【a,19】,【b,22】
    rdd.aggregateByKey(5)(
      (x, y) => x + y,
      (x, y) => x + y
    ).collect().foreach(println)

    sc.stop()
  }

}

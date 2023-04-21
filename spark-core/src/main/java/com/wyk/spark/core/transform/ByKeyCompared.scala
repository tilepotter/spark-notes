package com.wyk.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName ByKeyCompared
 * @Author wangyingkang
 * @Date 2022/4/25 17:38
 * @Version 1.0
 * @Description reduceByKey、foldByKey、aggregateByKey、combineByKey 的区别?
 *              1.reduceByKey: 相同 key 的第一个数据不进行任何计算，分区内和分区间计算规则相同
 *              2.FoldByKey: 相同 key 的第一个数据和初始值进行分区内计算，分区内和分区间计算规则相同
 *              3.AggregateByKey:相同 key 的第一个数据和初始值进行分区内计算，分区内和分区间计算规则可以不相同
 *              4.CombineByKey:当计算时，发现数据结构不满足要求时，可以让第一个数据转换结构。分区内和分区间计算规则不相同。
 * */
object ByKeyCompared {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - (Key - Value类型)

    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3),
      ("b", 4), ("b", 5), ("a", 6)
    ), 2)

    /*
    reduceByKey:

         combineByKeyWithClassTag[V](
             (v: V) => v, // 第一个值不会参与计算
             func, // 分区内计算规则
             func, // 分区间计算规则
             )

    aggregateByKey :

        combineByKeyWithClassTag[U](
            (v: V) => cleanedSeqOp(createZero(), v), // 初始值和第一个key的value值进行的分区内数据操作
            cleanedSeqOp, // 分区内计算规则
            combOp,       // 分区间计算规则
            )

    foldByKey:

        combineByKeyWithClassTag[V](
            (v: V) => cleanedFunc(createZero(), v), // 初始值和第一个key的value值进行的分区内数据操作
            cleanedFunc,  // 分区内计算规则
            cleanedFunc,  // 分区间计算规则
            )

    combineByKey :

        combineByKeyWithClassTag(
            createCombiner,  // 相同key的第一条数据进行的处理函数
            mergeValue,      // 表示分区内数据的处理函数
            mergeCombiners,  // 表示分区间数据的处理函数
            )

     */


    rdd.reduceByKey(_ + _) // wordcount
    rdd.aggregateByKey(0)(_ + _, _ + _) // wordcount
    rdd.foldByKey(0)(_ + _) // wordcount
    rdd.combineByKey(v => v, (x: Int, y: Int) => x + y, (x: Int, y: Int) => x + y) // wordcount


    sc.stop()
  }
}

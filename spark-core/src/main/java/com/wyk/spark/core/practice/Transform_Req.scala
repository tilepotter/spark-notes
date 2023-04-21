package com.wyk.spark.core.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName Transform_Req
 * @Author wangyingkang
 * @Date 2022/5/6 15:24
 * @Version 1.0
 * @Description transform算子案例实操：
 *              1) 数据准备：agent.log:时间戳，省份，城市，用户，广告，中间字段使用空格分隔。
 *              2) 需求描述：统计出每一个省份每个广告被点击数量排行的 Top3
 * */
object Transform_Req {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

    val sc = new SparkContext(sparkConf)

    // 1. 获取原始数据：时间戳，省份，城市，用户，广告
    val rdd = sc.textFile("input/data/agent.log")

    // 2. 将原始数据进行结构的转换。方便统计
    //    时间戳，省份，城市，用户，广告 => ( ( 省份，广告 ), 1 )
    val mapRDD: RDD[((String, String), Int)] = rdd.map(
      line => {
        val fields = line.split(" ")
        ((fields(1), fields(4)), 1)
      })

    // 3. 将转换结构后的数据，进行分组聚合
    //    ( ( 省份，广告 ), 1 ) => ( ( 省份，广告 ), sum )
    val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)

    // 4. 将聚合的结果进行结构的转换
    //    ( ( 省份，广告 ), sum ) => ( 省份, ( 广告, sum ) )
    val newMapRDD: RDD[(String, (String, Int))] = reduceRDD.map(
      value => {
        (value._1._1, (value._1._2, value._2))
      })

    // 5. 将转换结构后的数据根据省份进行分组
    //    ( 省份, 【( 广告A, sumA )，( 广告B, sumB )】 )
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = newMapRDD.groupByKey()

    // 6. 将分组后的数据组内排序（降序），取前3名
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      value => {
        value.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      })

    // 7. 采集数据打印在控制台
    resultRDD.collect().foreach(println)

    sc.stop()
  }
}

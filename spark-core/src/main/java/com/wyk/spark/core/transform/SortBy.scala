package com.wyk.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName SortBy
 * @Author wangyingkang
 * @Date 2022/4/11 17:40
 * @Version 1.0
 * @Description sortBy、sortByKey 参数ascending(是否升序)：false，使用降序排序 ｜｜ true，使用生序排序
 *              sortBy方法可以根据指定的规则对数据源中的数据进行排序，默认为升序，第二个参数可以改变排序的方式
 *              sortBy默认情况下，不会改变分区。但是中间存在shuffle操作
 * */
object SortBy {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    /**
     * 1.sortByKey : 按照键进行排序
     */
    val list01 = List((100, "hadoop"), (90, "spark"), (120, "storm"))

    sc.parallelize(list01, 2).sortByKey(ascending = true).foreach(println)

    println("---------------------------------------------------")
    /**
     * 2.sortBy : 按照指定元素进行排序
     */
    val list02 = List(("hadoop", 100), ("spark", 90), ("storm", 120))
    sc.parallelize(list02, 2).sortBy(x => x._2, ascending = false)
      .foreach(println(_))
  }

}

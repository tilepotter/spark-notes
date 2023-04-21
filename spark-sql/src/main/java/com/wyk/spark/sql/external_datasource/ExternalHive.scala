package com.wyk.spark.sql.external_datasource

import org.apache.spark.sql.SparkSession

/**
 * @ObjectName ExternalHive
 * @Author wangyingkang
 * @Date 2022/6/2 10:27
 * @Version 1.0
 * @Description Spark读取外部Hive
 * */
object ExternalHive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "hdfs://localhost:8020/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    // 使用SparkSQL连接外置的Hive
    // 1. 拷贝hive-site.xml文件到resources下，若还是报错找不到表，需要把hive-site.xml拷贝到target/classes下
    // 2. 启用Hive的支持
    // 3. 增加对应的依赖关系（包含MySQL驱动）
    spark.sql("show databases").show()

    spark.sql("select * from test.consumer").show()

    spark.close()
  }
}

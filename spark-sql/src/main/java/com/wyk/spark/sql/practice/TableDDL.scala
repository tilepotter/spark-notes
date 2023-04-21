package com.wyk.spark.sql.practice

import org.apache.spark.sql.SparkSession

/**
 * @ObjectName CreateTbale
 * @Author wangyingkang
 * @Date 2022/6/2 11:08
 * @Version 1.0
 * @Description SparkSQL练习；在 Hive 中创建表,，并导入数据。 一共有 3 张表: 1 张用户行为表，1 张城市表，1 张产品表
 * */
object TableDDL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSQL")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "hdfs://localhost:8020/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("use test")

    //创建表user_visit_action
    spark.sql(
      """
        |CREATE TABLE `user_visit_action`(
        |  `date` string,
        |  `user_id` bigint,
        |  `session_id` string,
        |  `page_id` bigint,
        |  `action_time` string,
        |  `search_keyword` string,
        |  `click_category_id` bigint,
        |  `click_product_id` bigint,
        |  `order_category_ids` string,
        |  `order_product_ids` string,
        |  `pay_category_ids` string,
        |  `pay_product_ids` string,
        |  `city_id` bigint)
        |row format delimited fields terminated by '\t'
        |""".stripMargin
    )

    //加载数据到user_visit_action
    spark.sql(
      """
        |load data local inpath 'input/data/user_visit_action.txt' into table test.user_visit_action
        |""".stripMargin
    )

    //创建表product_info
    spark.sql(
      """
        |CREATE TABLE `product_info`(
        |  `product_id` bigint,
        |  `product_name` string,
        |  `extend_info` string)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)

    //加载数据到product_info
    spark.sql(
      """
        |load data local inpath 'input/data/product_info.txt' into table test.product_info
        |""".stripMargin)

    //创建表city_info
    spark.sql(
      """
        |CREATE TABLE `city_info`(
        |  `city_id` bigint,
        |  `city_name` string,
        |  `area` string)
        |row format delimited fields terminated by '\t'
            """.stripMargin)

    //加载数据到city_info
    spark.sql(
      """
        |load data local inpath 'input/data/city_info.txt' into table test.city_info
            """.stripMargin)

    spark.sql("select * from test.city_info").show()

    spark.close()
  }
}

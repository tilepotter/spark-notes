package com.wyk.spark.sql.practice

import org.apache.spark.sql.SparkSession

/**
 * @ObjectName Top3
 * @Author wangyingkang
 * @Date 2022/6/2 11:33
 * @Version 1.0
 * @Description 计算热门商品：这里的热门商品是从点击量的维度来看的，计算各个区域前三大热门商品
 * */
object Practice01_Top3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSQL_Test").master("local[*]")
      .config("spark.sql.warehouse.dir", "hdfs://localhost:8020/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("use test")

    spark.sql(
      """
        |select
        |    *
        |from (
        |    select
        |        *,
        |        rank() over( partition by area order by clickCnt desc ) as rank
        |    from (
        |        select
        |           area,
        |           product_name,
        |           count(*) as clickCnt
        |        from (
        |            select
        |               a.*,
        |               p.product_name,
        |               c.area,
        |               c.city_name
        |            from user_visit_action a
        |            join product_info p on a.click_product_id = p.product_id
        |            join city_info c on a.city_id = c.city_id
        |            where a.click_product_id > -1
        |        ) t1 group by area, product_name
        |    ) t2
        |) t3 where rank <= 3
        |""".stripMargin).show()

    spark.close()
  }
}

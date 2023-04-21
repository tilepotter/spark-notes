package com.wyk.spark.sql.structured_api

import com.wyk.spark.sql.entity.Emp
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * @ClassName DataSet_InternalData
 * @Author wangyingkang
 * @Date 2022/4/12 16:32
 * @Version 1.0
 * @Description 由内部数据集创建DataSet
 * */
object DataSet_InternalData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Spark-SQL").master("local[2]").getOrCreate()
    // 1.需要导入隐式转换
    import spark.implicits._


    // 2.由内部数据集创建 Dataset
    val seq: Seq[Emp] = Seq(Emp("ALLEN", 12, "女", "2", 123.09), Emp("Bob", 28, "男", "1", 111.09))

    val caseClassDS: Dataset[Emp] = seq.toDS()

    caseClassDS.show()

    // 3.由RDD创建Dataset
    val rdd: RDD[Emp] = spark.sparkContext.makeRDD(List(Emp("ALLEN", 12, "女", "2", 123.09)
      , Emp("Bob", 28, "男", "1", 111.09)), 2)

    //rdd转换为dataset
    val ds: Dataset[Emp] = rdd.toDS()

    ds.show()
  }
}

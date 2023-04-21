package com.wyk.spark.core.serial

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ObjectName Serial_01
 * @Author wangyingkang
 * @Date 2022/5/19 17:39
 * @Version 1.0
 * @Description
 * */
object Serial_01 {
  def main(args: Array[String]): Unit = {
    //创建spark运行配置对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Test")

    //创建spark上下文环境对象（连接对象）
    val sc: SparkContext = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    val user = new User()

    //Task not serializable
    //object not serializable (class: com.wyk.spark.core.serial.Serial_01$User, value: com.wyk.spark.core.serial.Serial_01$User@6fc3e1a4)
    rdd.foreach(
      num => {
        println("age = " + (user.age + num))
      }
    )

    sc.stop()
  }

  class User extends Serializable {

    var age: Int = 10
  }
}

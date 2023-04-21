package com.wyk.spark.streaming.util

import java.io.InputStreamReader
import java.util.Properties

/**
 * @ObjectName PropertiesUtil
 * @Author wangyingkang
 * @Date 2022/8/30 16:09
 * @Version 1.0
 * @Description
 * */
object PropertiesUtil {
  def load(propertiesName: String): Properties = {
    val prop = new Properties()
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName), "UTF-8"))
    prop
  }
}

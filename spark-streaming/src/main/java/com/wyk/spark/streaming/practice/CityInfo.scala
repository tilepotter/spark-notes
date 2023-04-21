package com.wyk.spark.streaming.practice

/**
 * @ObjectName CityInfo
 * @Author wangyingkang
 * @Date 2022/8/30 16:13
 * @Version 1.0
 * @Description
 * */

/*
* 城市信息表 *
* @param city_id
* @param city_name
* @param area
*/
case class CityInfo(city_id: Long,
                    city_name: String,
                    area: String)

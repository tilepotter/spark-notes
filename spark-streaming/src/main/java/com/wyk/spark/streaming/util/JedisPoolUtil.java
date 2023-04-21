package com.wyk.spark.streaming.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @ClassName JedisPollUtil
 * @Author wangyingkang
 * @Date 2022/4/14 15:35
 * @Version 1.0
 * @Description
 **/
public class JedisPoolUtil {

    /*
     * 声明为 volatile 防止指令重排序
     */
    private static volatile JedisPool jedisPool = null;
    private static final String HOST = "hdp01";
    private static final int PORT = 6379;
    private static final int TIMEOUT_MS = 30000;
    private static final String AUTH = "admin";
    private static final int DATABASE = 15;

    /**
     * 双重检查锁实现懒汉式单例
     *
     * @return jedis
     */
    public static Jedis getConnection() {
        if (jedisPool == null) {
            synchronized (JedisPoolUtil.class) {
                if (jedisPool == null) {
                    JedisPoolConfig config = new JedisPoolConfig();
                    config.setMaxTotal(30);
                    config.setMaxIdle(10);
                    jedisPool = new JedisPool(config, HOST, PORT, TIMEOUT_MS, AUTH, DATABASE);
                }
            }
        }
        return jedisPool.getResource();
    }
}


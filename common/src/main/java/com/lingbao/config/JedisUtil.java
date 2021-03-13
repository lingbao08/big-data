package com.lingbao.config;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author lingbao08
 * @DESCRIPTION
 * @create 2019-10-23 18:24
 **/

public class JedisUtil {

    public static final JedisPoolConfig config = new JedisPoolConfig();

    private JedisUtil() {
        config.setMaxTotal(50);
        // 设置空闲时池中保有的最大连接数（可选）
        config.setMaxIdle(10);
    }

    public static Jedis getJedis() {
        // 2、设置连接池对象
        JedisPool pool = new JedisPool(config, "127.0.0.1", 6379);
        return pool.getResource();
    }
}

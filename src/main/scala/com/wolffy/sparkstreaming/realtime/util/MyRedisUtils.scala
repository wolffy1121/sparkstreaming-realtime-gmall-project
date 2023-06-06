package com.wolffy.sparkstreaming.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object MyRedisUtils {

    var jedisPool: JedisPool = null

    def main(args: Array[String]): Unit = {
        val jedisClient: Jedis = MyRedisUtils.getJedisClient
        val str: String = jedisClient.ping()
        println(str)
    }

    def getJedisClient : Jedis = {
        if(jedisPool == null ){
            var host : String = MyPropertiesUtils("redis.host")
            var port : String = MyPropertiesUtils("redis.port")
            val jedisPoolConfig = new JedisPoolConfig()
            jedisPoolConfig.setMaxTotal(1000) //最大连接数
            jedisPoolConfig.setMaxIdle(20) //最大空闲
            jedisPoolConfig.setMinIdle(20) //最小空闲
            jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
            jedisPoolConfig.setMaxWaitMillis(5000) //忙碌时等待时长 毫秒
            jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试

            jedisPool =  new JedisPool(jedisPoolConfig,host,port.toInt)
        }
        return jedisPool.getResource()
    }

    /**
     * close jedis pool
     *
     * @param jedis
     */
    def close(jedis: Jedis): Unit = {
        if (jedis != null) {
            jedis.close()
        }
    }


}

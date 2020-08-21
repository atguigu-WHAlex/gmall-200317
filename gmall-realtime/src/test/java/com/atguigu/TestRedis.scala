package com.atguigu

import com.atguigu.utils.RedisUtil
import redis.clients.jedis.Jedis

object TestRedis {

  def main(args: Array[String]): Unit = {

    val client: Jedis = RedisUtil.getJedisClient
    client.set("zhongwen","中文")

    client.close()

  }

}

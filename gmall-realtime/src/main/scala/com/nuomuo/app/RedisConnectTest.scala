package com.nuomuo.app

import redis.clients.jedis.Jedis

object RedisConnectTest {
  def main(args: Array[String]): Unit = {

    val jedis = new Jedis("node3", 6379)
    val poing: String = jedis.ping()
    println(poing)

    jedis.close()

  }

}

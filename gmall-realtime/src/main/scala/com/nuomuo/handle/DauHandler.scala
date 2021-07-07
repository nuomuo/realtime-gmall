package com.nuomuo.handle

import com.nuomuo.bean.StartUpLog
import com.nuomuo.utils.PropertiesUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

object DauHandler {
  val properties: Properties = PropertiesUtil.load("config.properties")
  val redis_host: String = properties.getProperty("redis.host")
  val redis_post: Int = Integer.parseInt(properties.getProperty("redis.port"))

  def saveToRedis(value: DStream[StartUpLog]): Unit = {
    value.foreachRDD((rdd: RDD[StartUpLog]) => {
      rdd.foreachPartition((partition: Iterator[StartUpLog]) => {
        val jedis = new Jedis(redis_host, redis_post)

        partition.foreach((log: StartUpLog) => {
          val keyName: String = "DAU:"+log.logDate
          jedis.sadd(keyName,log.mid)
        })

      })
    })
  }

  def filterByRedis(logDStream: DStream[StartUpLog], ssc: StreamingContext): DStream[StartUpLog] = {
    val properties: Properties = PropertiesUtil.load("config.properties")
    val redis_host: String = properties.getProperty("redis.host")
    val redis_post: Int = Integer.parseInt(properties.getProperty("redis.port"))

    // 方式一
    /*
    logDStream.filter((log: StartUpLog) => {
        // 创建redis连接 在 excutor 端进行
        val jedisClient: Jedis = new Jedis(redis_host, redis_post)
        //redisKey
        val rediskey: String = "DAU:" + log.logDate
        //对比数据，重复的去掉，不重的留下来
        val boolean: lang.Boolean = jedisClient.sismember(rediskey, log.mid)
        //关闭连接
        jedisClient.close()
        !boolean
    })
    */
    // 方式二：每个分区创建一个 redis 连接

    logDStream.mapPartitions((partition: Iterator[StartUpLog]) => {
      // 创建redis连接 在 excutor 端进行
      val jedis = new Jedis(redis_host, redis_post)
      val logs: Iterator[StartUpLog] = partition.filter((log: StartUpLog) => {
        val keyName: String = "DAU:" + log.logDate
        !jedis.sismember(keyName, log.mid)
      })
      jedis.close()
      logs
    })


    // 方式三：每个批次创建一个 redis 连接
    logDStream.transform((rdd: RDD[StartUpLog]) => {
      val format = new SimpleDateFormat("yyyy-DD-mm")
      // 创建 redis 连接 在 driver 端进行
      val jedis = new Jedis(redis_host, redis_post)
      val keyName: String = "DAU:"+ format.format(new Date(System.currentTimeMillis()))
      val logs: util.Set[String] = jedis.smembers(keyName)

      val mids: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(logs)

      val midsRDD: RDD[StartUpLog] = rdd.filter((log: StartUpLog) => {
        !mids.value.contains(log.mid)
      })
      jedis.close()
      midsRDD
    })

  }

}

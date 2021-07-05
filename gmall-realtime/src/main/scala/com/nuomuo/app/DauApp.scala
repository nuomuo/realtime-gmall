package com.nuomuo.app

import com.alibaba.fastjson.JSON
import com.nuomuo.GmallConstants
import com.nuomuo.bean.StartUpLog
import com.nuomuo.handle.DauHandler
import com.nuomuo.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.phoenix.spark._
import org.apache.spark.rdd.RDD


/**
 * 日活 将去重后的 mid（启动日志中的设备id）存放到 redis 中 ，并将完整信息 存放到 hbase 中
 *
 */
object DauApp {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    val ssc = new StreamingContext(conf, Seconds(5))


    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    /*
    kafkaStream.foreachRDD((rdd: RDD[ConsumerRecord[String, String]]) =>{
      rdd.foreach((f: ConsumerRecord[String, String]) =>  println(f.value()))
    })
    */

    val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    // 将 ConsumerRecord 转化为 样例类
    val logDStream: DStream[StartUpLog] = kafkaStream.mapPartitions((partition: Iterator[ConsumerRecord[String, String]]) => {
      val logs: Iterator[StartUpLog] = partition.map((consumerRecour: ConsumerRecord[String, String]) => {
        val value: String = consumerRecour.value()
        val log: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])
        val date: String = simpleDateFormat.format(new Date(log.ts))
        log.logDate = date.split(" ")(0)
        log.logHour = date.split(" ")(1)
        log
      })
      logs
    })

    // todo 1. 先进行批次间的去重 ：
    // 每一个批次的数据 都与 redis 中的数据进行 比较，如果redis中已有，则过滤掉该数据，如果没有则保留
    val redisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(logDStream, ssc)

    // todo 2. 在进行批次内的去重 ：
    val tupleDStream: DStream[((String, Long), StartUpLog)] = redisDStream.mapPartitions((partition: Iterator[StartUpLog]) => {
      partition.map((log: StartUpLog) => {
        ((log.mid, log.ts), log)
      })
    })

    val listDStream: DStream[((String, Long), List[StartUpLog])] = tupleDStream.groupByKey().mapValues((iters: Iterable[StartUpLog]) => {
      val list: List[StartUpLog] = iters.toList
      list.sortWith((it1: StartUpLog, it2: StartUpLog) => it1.ts < it2.ts).take(1)
    })

    val value: DStream[StartUpLog] = listDStream.flatMap(_._2)

    value.print()


    // todo 保存到 redis

    DauHandler.saveToRedis(value)

    value.foreachRDD((rdd: RDD[StartUpLog]) => {
      rdd.saveToPhoenix(
        "GMALL2021_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
        HBaseConfiguration.create,
        Some("node1,node2,node3:2181"))
    })

    ssc.start()

    ssc.awaitTermination()



  }
}

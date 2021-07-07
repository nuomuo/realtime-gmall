package com.nuomuo.app

import com.alibaba.fastjson.JSON
import com.nuomuo.GmallConstants
import com.nuomuo.bean.{CouponAlertInfo, EventLog}
import com.nuomuo.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._
import java.text.SimpleDateFormat
import java.util
import java.util.Date

/**
 * 拿事件日志的数据：
 * 5分钟内 在同一个设备上有 超过3个以上不同的用户 领取优惠卷 且 该设备上没有浏览任何商品
 * 分析：
 * 使用 窗口函数 window(Minutes(5)) 取 5 分钟的数据
 * 将数据 groupByKey key 为 设备 id  ---> (mid,iter()) --> 遍历 iter 如果 iter 里有浏览商品的记录排除 该设备
 * 如果 iter 里 超过了3 个不同的用户 领取优惠卷 加入报警信息 （需要对用户进行去重，同一个用户可以多次领取）
 *
 */
object AlertAPP {
  def main(args: Array[String]): Unit = {
    // 1 创建 sparkConfig
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertAPP")

    // 2 创建 streamingContext
    val ssc = new StreamingContext(conf, Seconds(3))
    // 3 创建 kafka 连接
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)
    // 4 开窗
    val windowDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.window(Minutes(5))
    // 5 将 kafka 读取的json 字符串 转化为 样例类

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
    val transDStream: DStream[(String, EventLog)] = windowDStream.mapPartitions((partition: Iterator[ConsumerRecord[String, String]]) => {
      partition.map((record: ConsumerRecord[String, String]) => {
        val log: EventLog = JSON.parseObject(record.value(), classOf[EventLog])
        val logStr: String = dateFormat.format(new Date(log.ts))
        log.logDate = logStr.split(" ")(0)
        log.logHour = logStr.split(" ")(1)
        (log.mid, log)
      })
    })

    val midDStream: DStream[(String, Iterable[EventLog])] = transDStream.groupByKey()

    val waitSendDStream: DStream[(Boolean, CouponAlertInfo)] = midDStream.map {
      case (mid, iter) => {
        val mids = new util.HashSet[String]()
        val itemIds: util.HashSet[String] = new util.HashSet[String]()
        //创建List集合用来保存用户行为事件
        val events: util.ArrayList[String] = new util.ArrayList[String]()
        var flag = true
        breakable {
          iter.toList.foreach((log: EventLog) => {
            if ("clickItem".equals(log.evid)) {
              break()
              flag = false
            } else if ("coupon".equals(log.evid)) {
              mids.add(log.mid)
              itemIds.add(log.itemid)
            }

            events.add(log.evid)
          })
        }
        // 生成 发送到 ES 的 消息
        ((mids.size() >= 3 && flag), CouponAlertInfo(mid, mids, itemIds, events, System.currentTimeMillis()))
      }
    }

    val sendDStream: DStream[CouponAlertInfo] = waitSendDStream.filter(_._1).map(_._2)
    //9.将预警数据写入ES
    sendDStream.foreachRDD((rdd: RDD[CouponAlertInfo]) =>{
      rdd.foreachPartition((iter: Iterator[CouponAlertInfo]) =>{

        val indexName: String =GmallConstants.ES_ALERT_INDEXNAME
        val list: List[(String, CouponAlertInfo)] = iter.toList.map((alert: CouponAlertInfo) => {
          (alert.mid + alert.ts / 1000 / 60, alert)
        })
        MyEsUtil.insertBulk(indexName,list)
      })
    })

    //10.开启
    ssc.start()
    ssc.awaitTermination()



  }



}

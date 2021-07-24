package com.nuomuo.app

import com.alibaba.fastjson.JSON
import com.nuomuo.GmallConstants
import com.nuomuo.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.nuomuo.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.native.Serialization
import redis.clients.jedis.Jedis

import java.text.SimpleDateFormat
import java.util
import java.util.Date
import collection.JavaConverters._
/**
 * 将业务数据 同步到 kafka 中：订单表 订单详细表 用户表
 * 将 订单表 order_info join order_detail
 */
object SaleDetailApp {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Seconds(3))

    val orderKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER, streamingContext)

    val detailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, streamingContext)

    // 1 将 DStream 转化成 样例类
    val orderInfoStream: DStream[(String, OrderInfo)] =
      orderKafkaDStream.mapPartitions((partition: Iterator[ConsumerRecord[String, String]]) => {
      partition.map((record: ConsumerRecord[String, String]) => {
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        orderInfo.create_date = orderInfo.create_time.split(" ")(0)
        orderInfo.create_hour = orderInfo.create_time.split(" ")(1).split(":")(0)
        (orderInfo.id, orderInfo)
      })
    })
    val orderDetailStream: DStream[(String, OrderDetail)] =
      detailKafkaDStream.mapPartitions((partition: Iterator[ConsumerRecord[String, String]]) => {
      partition.map((record: ConsumerRecord[String, String]) => {
        val orderDetail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
        (orderDetail.order_id, orderDetail)
      })
    })

    //val value: DStream[(String, (OrderInfo, OrderDetail))] = orderInfoStream.join(orderDetailStream)

    val fullOutJoinStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoStream.fullOuterJoin(orderDetailStream)

    val noUserDStream: DStream[SaleDetail] = fullOutJoinStream.mapPartitions((partition: Iterator[(String, (Option[OrderInfo], Option[OrderDetail]))]) => {
      val jedis = new Jedis("node1", 6379)
      val list: util.ArrayList[SaleDetail] = new util.ArrayList[SaleDetail]()
      partition.foreach((item: (String, (Option[OrderInfo], Option[OrderDetail]))) =>{

        val orderId: String = item._1
        val orderInfoOption: Option[OrderInfo] = item._2._1
        val orderDetailOption: Option[OrderDetail] = item._2._2

        val orderInfoRedisKey: String = "orderInfo:" + orderId
        val orderDetailRedisKey: String = "orderDetail:" + orderId



        // 在 orderInfo 有数据的情况下
        if (orderInfoOption.isDefined) {
          val orderInfo: OrderInfo = orderInfoOption.get
          // 先将 两个流同一批次下 匹配好的 进行组合
          if (orderDetailOption.isDefined) {
            val saleDetail = new SaleDetail(orderInfo, orderDetailOption.get)
            list.add(saleDetail)
          }
          // 将 orderInfo 存入 redis 中
          if (!jedis.exists(orderInfoRedisKey)) {
            // 将样例类 转化成 JSON 字符串
            import org.json4s.native.Serialization
            implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
            val orderInfoJson: String = Serialization.write(orderInfo)
            jedis.set(orderInfoRedisKey, orderInfoJson)
          }
          // 从 orderDetail的缓存中找 是否有需要 与本 orderInfo 结合的
          if (jedis.exists(orderDetailRedisKey)) {
            val details: util.Set[String] = jedis.smembers(orderDetailRedisKey)
            details.asScala.foreach((str: String) => {
              val orderDetail: OrderDetail = JSON.parseObject(str, classOf[OrderDetail])
              val saleDetail = new SaleDetail(orderInfo, orderDetail)
              list.add(saleDetail)
            })
            // 删除


          }
        } else {
          // 没有 orderInfo 数据 拿到 orderDetailOption
          // 去 缓存中 找 KEY= orderInfo:orderId 找到 则 匹配 找不到 存入缓存
          val orderDetail: OrderDetail = orderDetailOption.get // 这里的 Option一定有值

          if (jedis.exists(orderInfoRedisKey)) {
            val str: String = jedis.get(orderInfoRedisKey)
            val orderInfo: OrderInfo = JSON.parseObject(str, classOf[OrderInfo])
            val saleDetail = new SaleDetail(orderInfo, orderDetail)
            list.add(saleDetail)

          } else {
            import org.json4s.native.Serialization
            implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
            val orderDetailStr: String = Serialization.write(orderDetail)
            jedis.sadd(orderInfoRedisKey, orderDetailStr)
          }
        }
      })
      jedis.close()
      list.asScala.toIterator
    })


    // 添加 用户信息
    val saleDetailDStream: DStream[SaleDetail] = noUserDStream.mapPartitions((partition: Iterator[SaleDetail]) => {
      //a.获取redis连接
      val jedisClient: Jedis = new Jedis("node1", 6379)
      //b.查库
      val details: Iterator[SaleDetail] = partition.map((saleDetail: SaleDetail) => {
        //根据key获取数据
        val userInfoKey: String = "userInfo:" + saleDetail.user_id
        val userInfoJson: String = jedisClient.get(userInfoKey)
        //将数据转换为样例类
        val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
        saleDetail.mergeUserInfo(userInfo)
        saleDetail
      })
      //关闭连接
      jedisClient.close()
      details
    })

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    saleDetailDStream.foreachRDD((rdd: RDD[SaleDetail]) =>{
      rdd.foreachPartition((partition: Iterator[SaleDetail]) =>{

        //索引名
        val indexName: String = GmallConstants.ES_DETAIL_INDEXNAME+"-"+sdf.format(new Date(System.currentTimeMillis()))

        val list: List[(String, SaleDetail)] = partition.toList.map((saleDetail: SaleDetail) => {
          (saleDetail.order_detail_id, saleDetail)
        })

        MyEsUtil.insertBulk(indexName,list)
      })
    })

    //9.开启任务
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
package com.atguigu.app

import java.time.LocalDate
import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import redis.clients.jedis.Jedis
import org.json4s.native.Serialization

import collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object SaleDetailApp {

  def main(args: Array[String]): Unit = {

    //TODO 创建StreamingContext
    val sparkConf: SparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //TODO 消费Kafka order_info和order_detail 主题数据
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_INFO, ssc)
    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_DETAIL, ssc)

    //TODO 将每一行数据转换为样例类元组(order_id,order_info) (order_id,order_detail)
    val orderIdToInfoDStream: DStream[(String, OrderInfo)] = orderInfoKafkaDStream.map(record => {
      //a.转换为样例类对象
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      //b.给日期及小时字段重新赋值
      val create_time: String = orderInfo.create_time //2020-08-18 04:24:04
      val timeArr: Array[String] = create_time.split(" ")
      orderInfo.create_date = timeArr(0)
      orderInfo.create_hour = timeArr(1).split(":")(0)
      //c.给联系人手机号脱敏
      val telTuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
      orderInfo.consignee_tel = telTuple._1 + "*******"
      //d.返回数据
      (orderInfo.id, orderInfo)
    })

    val orderIdToDetailDStream: DStream[(String, OrderDetail)] = orderDetailKafkaDStream.map(record => {
      //JSON 转换
      val detail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      //返回数据
      (detail.order_id, detail)
    })

    //TODO 做JOIN
    //    val value: DStream[(String, (OrderInfo, OrderDetail))] = orderIdToInfoDStream.join(orderIdToDetailDStream)
    //    val value1: DStream[(String, (OrderInfo, Option[OrderDetail]))] = orderIdToInfoDStream.leftOuterJoin(orderIdToDetailDStream)
    //    val value2: DStream[(String, (Option[OrderInfo], OrderDetail))] = orderIdToInfoDStream.rightOuterJoin(orderIdToDetailDStream)
    val orderIdToInfoAndDetailDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderIdToInfoDStream.fullOuterJoin(orderIdToDetailDStream)

    //TODO 使用mapPartitions代替map操作,减少连接的创建与释放
    val noUserSaleDetail: DStream[SaleDetail] = orderIdToInfoAndDetailDStream.mapPartitions(iter => {

      //获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //创建集合用于存放JOIN上(包含当前批次,以及两个流中跟前置批次JOIN上的)的数据
      val details = new ListBuffer[SaleDetail]
      //导入样例类对象转换为JSON的隐式
      implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats

      //处理分区中每一条数据
      iter.foreach { case ((orderId, (infoOpt, detailOpt))) =>

        //定义info&detail数据存入Redis中的Key
        val infoRedisKey = s"order_info:$orderId"
        val detailRedisKey = s"order_detail:$orderId"

        //1.infoOpt有值
        if (infoOpt.isDefined) {

          //获取infoOpt中的数据
          val orderInfo: OrderInfo = infoOpt.get

          //1.1 detailOpt也有值
          if (detailOpt.isDefined) {
            //获取detailOpt中的数据
            val orderDetail: OrderDetail = detailOpt.get
            //结合放入集合
            details += new SaleDetail(orderInfo, orderDetail)
          }

          //1.2 将orderInfo转换JSON字符串写入Redis
          // JSON.toJSONString(orderInfo)  编译报错
          val orderInfoJson: String = Serialization.write(orderInfo)
          jedisClient.set(infoRedisKey, orderInfoJson)
          jedisClient.expire(infoRedisKey, 100)

          //1.3 查询OrderDetail流前置批次数据
          val orderDetailJsonSet: util.Set[String] = jedisClient.smembers(detailRedisKey)
          //          if (!orderDetailJsonSet.isEmpty){
          orderDetailJsonSet.asScala.foreach(orderDetailJson => {
            //将orderDetailJson转换为样例类对象
            val orderDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
            //结合存放入集合
            details += new SaleDetail(orderInfo, orderDetail)
          })
          //}

        } else {
          //2.infoOpt没有值

          //获取detailOpt中的数据
          val orderDetail: OrderDetail = detailOpt.get

          //查询OrderInfo流前置批次数据
          val orderInfoJson: String = jedisClient.get(infoRedisKey)
          if (orderInfoJson != null) {

            //2.1 查询有数据
            //将orderInfoJson转换为样例类对象
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
            //结合写出
            details += new SaleDetail(orderInfo, orderDetail)

          } else {

            //2.2 查询没有结果,将当前的orderDetail写入Redis
            val orderDetailJson: String = Serialization.write(orderDetail)
            jedisClient.sadd(detailRedisKey, orderDetailJson)
            jedisClient.expire(detailRedisKey, 100)
          }
        }
      }

      //释放连接
      jedisClient.close()
      //最终的返回值
      details.toIterator
    })

    //TODO 根据userID查询Redis数据,将用户信息补充完整
    val saleDetailDStream: DStream[SaleDetail] = noUserSaleDetail.mapPartitions(iter => {

      //a.获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient

      //b.遍历iter,对每一条数据进行查询Redis操作,补充用户信息
      val details: Iterator[SaleDetail] = iter.map(noUserSaleDetail => {
        //查询Redis
        val userInfoJson: String = jedisClient.get(s"userInfo:${noUserSaleDetail.user_id}")
        //将userInfoJson转换为样例类
        val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
        //补充信息
        noUserSaleDetail.mergeUserInfo(userInfo)
        //返回数据
        noUserSaleDetail
      })

      //c.释放连接
      jedisClient.close()

      //e.返回数据
      details
    })

    //TODO 将三张表JOIN的结果写入ES
    saleDetailDStream.foreachRDD(rdd => {

      rdd.foreachPartition(iter => {

        //根据当天时间创建索引名称
        val today: String = LocalDate.now().toString
        val indexName: String = s"${GmallConstants.GMALL_ES_SALE_DETAIL_PRE}-$today"

        //将orderDetailId作为ES中索引的docId
        val detailIdToSaleDetailIter: Iterator[(String, SaleDetail)] = iter.map(saleDetail => (saleDetail.order_detail_id, saleDetail))

        //调用ES工具类写入数据
        MyEsUtil.insertByBulk(indexName, "_doc", detailIdToSaleDetailIter.toList)

      })

    })

    //TODO 打印测试
    //    value.print(100)
    //    noUserSaleDetail.print(100)
    //    saleDetailDStream.print(100)

    //TODO 启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}

package com.atguigu.app

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

object AlertApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf和StreamingContext
    val sparkConf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //2.读取Kafka 事件主题数据创建流
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_EVENT, ssc)

    //3.转换为样例类对象
    val eventLogDStream: DStream[EventLog] = kafkaDStream.map(record => {

      //a.转换
      val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])

      //b.处理日志日期和小时
      val dateHour: String = sdf.format(new Date(eventLog.ts))
      val dateHourArr: Array[String] = dateHour.split(" ")
      eventLog.logDate = dateHourArr(0)
      eventLog.logHour = dateHourArr(1)

      //c.返回结果
      eventLog
    })


    //4.开5min窗
    val windowDStream: DStream[EventLog] = eventLogDStream.window(Minutes(5))

    //5.按照mid做分组处理
    val midToEventLogIterDStream: DStream[(String, Iterable[EventLog])] = windowDStream.map(eventLog => (eventLog.mid, eventLog)).groupByKey()

    //6.对单条数据做处理:
    //6.1 三次及以上用不同账号(登录并领取优惠劵)：对uid去重
    //6.2 没有浏览商品：反面考虑,如果有浏览商品,当前mid不产生预警日志
    val couponAlertInfoDStream: DStream[CouponAlertInfo] = midToEventLogIterDStream.map { case (mid, logIter) =>

      //a.创建Set用于存放领券的UID
      val uidSet: util.HashSet[String] = new java.util.HashSet[String]()
      //创建Set用于存放领券涉及的商品ID
      val itemIds: util.HashSet[String] = new java.util.HashSet[String]()
      //创建List用于存放用户行为
      val events = new util.ArrayList[String]()

      //定义标志位用于标识是否有浏览行为
      var noClick: Boolean = true

      //b.遍历logIter
      breakable(
        logIter.foreach(eventLog => {

          //提取事件类型
          val evid: String = eventLog.evid

          //将事件添加至集合
          events.add(evid)

          //判断当前数据是否为领券行为
          if ("coupon".equals(evid)) {
            itemIds.add(eventLog.itemid)
            uidSet.add(eventLog.uid)

          } else if ("clickItem".equals(evid)) {
            noClick = false
            break()
          }
        })
      )

      //根据条选择生成预警日志
      if (uidSet.size() >= 3 && noClick) {
        //满足条件生成预警日志
        CouponAlertInfo(mid, uidSet, itemIds, events, System.currentTimeMillis())
      } else {
        //不满足条件,则不生成预警日志
        null
      }
    }

    //打印测试
    //    eventLogDStream.print()

    //预警日志测试打印
    val filterAlertDStream: DStream[CouponAlertInfo] = couponAlertInfoDStream.filter(x => x != null)
    filterAlertDStream.cache()
    filterAlertDStream.print()

    //7.将生成的预警日志写入ES
    filterAlertDStream.foreachRDD(rdd => {

      rdd.foreachPartition(iter => {

        //转换数据结构,  预警日志 ===> (docID,预警日志)
        val docIdToData: Iterator[(String, CouponAlertInfo)] = iter.map(alertInfo => {
          val minutes: Long = alertInfo.ts / 1000 / 60
          (s"${alertInfo.mid}-$minutes", alertInfo)
        })

        //获取当前时间
        val date: String = LocalDate.now().toString
        MyEsUtil.insertByBulk(GmallConstants.GMALL_ES_ALERT_INFO_PRE + "_" + date,
          "_doc",
          docIdToData.toList)

      })

    })

    //8.启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}

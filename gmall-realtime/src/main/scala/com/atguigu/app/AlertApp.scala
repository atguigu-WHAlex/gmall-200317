package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.EventLog
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

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

    //打印测试
    eventLogDStream.print()

    //启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}

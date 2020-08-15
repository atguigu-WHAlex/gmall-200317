package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

object DauApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.读取Kafka Start主题的数据创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_START, ssc)

    //4.将读取的数据转换为样例类对象(logDate和logHour)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val startLogDStream: DStream[StartUpLog] = kafkaDStream.map(record => {

      //a.取出Value
      val value: String = record.value()

      //b.转换为样例类对象
      val startUpLog: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])

      //c.取出时间戳字段解析给logDate和logHour赋值
      val ts: Long = startUpLog.ts
      val dateHour: String = sdf.format(new Date(ts))
      val dateHourArr: Array[String] = dateHour.split(" ")
      startUpLog.logDate = dateHourArr(0)
      startUpLog.logHour = dateHourArr(1)

      //d.返回值
      startUpLog
    })

    //    startLogDStream.cache()
    //    startLogDStream.count().print()

    //5.根据Redis中保存的数据进行跨批次去重
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startLogDStream, ssc.sparkContext)

    //    filterByRedisDStream.cache()
    //    filterByRedisDStream.count().print()

    //6.对第一次去重后的数据做同批次去重
    val filterByGroupDStream: DStream[StartUpLog] = DauHandler.filterByGroup(filterByRedisDStream)

    filterByGroupDStream.cache()
    //    filterByGroupDStream.count().print()

    //7.将两次去重后的数据(mid)写入Redis
    DauHandler.saveMidToRedis(filterByGroupDStream)

    //8.将数据保存至HBase(Phoenix)
    filterByGroupDStream.foreachRDD(rdd => {
      rdd.saveToPhoenix("GMALL200317_DAU",
        classOf[StartUpLog].getDeclaredFields.map(_.getName.toUpperCase()),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    //9.启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}
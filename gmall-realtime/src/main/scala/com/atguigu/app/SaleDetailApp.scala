package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo}
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SaleDetailApp {

  def main(args: Array[String]): Unit = {

    //1.创建StreamingContext
    val sparkConf: SparkConf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //2.消费Kafka order_info和order_detail 主题数据
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_INFO, ssc)
    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_DETAIL, ssc)

    //3.将每一行数据转换为样例类元组(order_id,order_info) (order_id,order_detail)
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

    //4.做JOIN
    val value: DStream[(String, (OrderInfo, OrderDetail))] = orderIdToInfoDStream.join(orderIdToDetailDStream)
    val value1: DStream[(String, (OrderInfo, Option[OrderDetail]))] = orderIdToInfoDStream.leftOuterJoin(orderIdToDetailDStream)
    val value2: DStream[(String, (Option[OrderInfo], OrderDetail))] = orderIdToInfoDStream.rightOuterJoin(orderIdToDetailDStream)
    val value3: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderIdToInfoDStream.fullOuterJoin(orderIdToDetailDStream)

    //5.打印测试
    value.print(100)

    //6.启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}

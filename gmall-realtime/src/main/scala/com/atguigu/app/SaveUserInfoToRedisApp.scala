package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

object SaveUserInfoToRedisApp {

  def main(args: Array[String]): Unit = {

    //1.创建StreamingContext
    val sparkConf: SparkConf = new SparkConf().setAppName("SaveUserInfoToRedisApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //2.读取Kafka中 USER_INFO主题的数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_USER_INFO, ssc)

    //3.写入Redis
    kafkaDStream.foreachRDD(rdd => {

      //对分区操作,减少连接的创建与释放
      rdd.foreachPartition(iter => {

        //a.获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient

        //b.遍历iter,写入Redis
        iter.foreach(record => {
          //获取数据
          val userInfoJson: String = record.value()
          //转换为样例类对象
          val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
          //定义RedisKey
          val userRedisKey: String = s"userInfo:${userInfo.id}"
          //将数据写入Redis
          jedisClient.set(userRedisKey, userInfoJson)
        })

        //c.释放连接
        jedisClient.close()
      })
    })

    //4.启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}

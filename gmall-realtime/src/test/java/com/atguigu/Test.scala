package com.atguigu

import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Test {

  def main(args: Array[String]): Unit = {

    def func: (Seq[Int], Option[Int]) => Some[Int] = (nums: Seq[Int], states: Option[Int]) => {
      val curNum: Int = nums.sum
      val lastNum: Int = states.getOrElse(0)
      Some(curNum + lastNum)
    }

    //创建Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")

    //创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("./ck")

    //读取Kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("test", ssc)

    //计算WordCount
    kafkaDStream
      .map(_.value())
      .flatMap(_.split(" "))
      .map((_, 1))
      .updateStateByKey(func)
      .print()

    //启动
    ssc.start()
    ssc.awaitTermination()

  }

}

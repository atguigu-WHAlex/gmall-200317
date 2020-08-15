package com.atguigu

import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream

object Test2 {

  def getSSC: StreamingContext = {

    def func: (Seq[Int], Option[Int]) => Some[Int] = (nums: Seq[Int], states: Option[Int]) => {
      val curNum: Int = nums.sum
      val lastNum: Int = states.getOrElse(0)
      Some(curNum + lastNum)
    }

    //创建Spark配置信息
    val sparkConf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")

    //创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("./ck1")

    //读取Kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("test", ssc)

    //计算WordCount
    kafkaDStream
      .map(_.value())
      .flatMap(_.split(","))
      .map((_, 1))
      .updateStateByKey(func)
      .print()

    //返回结果
    ssc

  }

  def main(args: Array[String]): Unit = {

    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./ck1", () => getSSC)

    ssc.start()
    ssc.awaitTermination()

  }

}

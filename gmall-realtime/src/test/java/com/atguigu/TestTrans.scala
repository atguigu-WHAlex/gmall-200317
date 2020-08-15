package com.atguigu

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TestTrans {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val lineDStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)

    //Driver 打印一次
    println(s"11111111111111111${Thread.currentThread().getName}")

    lineDStream.transform(rdd => {

      //Driver 每个批次打印一次
      println(s"22222222222222222${Thread.currentThread().getName}")

      rdd.map(x => {
        //Executor 每条数据打印一次
        println(s"33333333333333333${Thread.currentThread().getName}")
        (x, 1)
      })
    }).print()

    ssc.start()
    ssc.awaitTermination()

  }

}

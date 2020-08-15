package com.atguigu.handler

import java.{lang, util}
import java.time.LocalDate

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler {

  //对第一次去重后的数据做同批次去重
  def filterByGroup(filterByRedisDStream: DStream[StartUpLog]): DStream[StartUpLog] = {

    //a.转换结构
    val midDateToStartLogDStream: DStream[(String, StartUpLog)] = filterByRedisDStream.map(startLog => {
      (s"${startLog.mid}-${startLog.logDate}", startLog)
    })

    //b.按照Key分组
    val midDateToStartLogDStreamIter: DStream[(String, Iterable[StartUpLog])] = midDateToStartLogDStream.groupByKey()

    //c.组内取时间戳最小的一条数据
    val midDateToStartLogDStreamList: DStream[(String, List[StartUpLog])] = midDateToStartLogDStreamIter.mapValues(iter => {
      iter.toList.sortWith(_.ts < _.ts).take(1)
    })

    //d.压平
    midDateToStartLogDStreamList.flatMap { case (_, list) =>
      list
    }

  }


  //根据Redis中保存的数据进行跨批次去重
  def filterByRedis(startLogDStream: DStream[StartUpLog], sc: SparkContext): DStream[StartUpLog] = {

    //方案一：单条数据过滤
    val value1: DStream[StartUpLog] = startLogDStream.filter(startLog => {
      //a.获取Redis连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //b.查询Redis中是否存在该Mid
      val exist: lang.Boolean = jedisClient.sismember(s"dau:${startLog.logDate}", startLog.mid)
      //c.归还连接
      jedisClient.close()
      //d.返回值
      !exist
    })

    //方案二：使用分区操作代替单条数据操作,减少连接数
    startLogDStream.mapPartitions(iter => {
      //a.获取连接
      val jedisClient: Jedis = RedisUtil.getJedisClient
      //b.过滤数据
      val filterIter: Iterator[StartUpLog] = iter.filter(startLog => {
        !jedisClient.sismember(s"dau:${startLog.logDate}", startLog.mid)
      })
      //c.归还连接
      jedisClient.close()
      //d.返回值
      filterIter
    })

    //方案三：每个批次获取一次Redis中的Set集合数据,广播至Executor
    val value3: DStream[StartUpLog] = startLogDStream.transform(rdd => {

      //a.获取Redis中的Set集合数据并广播,每个批次在Driver端执行一次
      val jedisClient: Jedis = RedisUtil.getJedisClient

      val today: String = LocalDate.now().toString
      val midSet: util.Set[String] = jedisClient.smembers(s"dau:$today")
      val midSetBC: Broadcast[util.Set[String]] = sc.broadcast(midSet)

      jedisClient.close()

      //b.在Executor端使用广播变量进行去重
      rdd.filter(startLog => {
        !midSetBC.value.contains(startLog.mid)
      })

    })
    //方法返回值
    //    value1
    value3
  }


  //将两次去重后的数据(mid)写入Redis
  def saveMidToRedis(startLogDStream: DStream[StartUpLog]): Unit = {

    startLogDStream.foreachRDD(rdd => {

      //使用foreachPartition代替foreach,减少连接的获取与释放
      rdd.foreachPartition(iter => {

        //a.获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient

        //b.操作数据
        iter.foreach(startLog => {
          val redisKey = s"dau:${startLog.logDate}"
          jedisClient.sadd(redisKey, startLog.mid)
        })

        //c.归还连接
        jedisClient.close()

      })

    })

  }

}

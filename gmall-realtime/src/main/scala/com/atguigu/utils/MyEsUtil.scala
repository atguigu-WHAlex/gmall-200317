package com.atguigu.utils

import java.util
import java.util.Objects

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, BulkResult, Index}
import collection.JavaConversions._

object MyEsUtil {

  private val ES_HOST = "http://hadoop102"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = _

  /**
    * 获取客户端
    *
    * @return jestclient
    */
  def getClient: JestClient = {
    if (factory == null) build()
    factory.getObject
  }

  /**
    * 关闭客户端
    */
  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
    * 建立连接
    */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT)
      .multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000)
      .readTimeout(10000)
      .build)
  }

  // 批量插入数据到ES
  def insertByBulk(indexName: String, typeName: String, list: List[(String, Any)]): Unit = {

    //集合非空
    if (list.nonEmpty) {

      //a.获取连接
      val client: JestClient = getClient

      //b.创建Bulk.Builder对象
      val builder: Bulk.Builder = new Bulk.Builder()
        .defaultIndex(indexName)
        .defaultType(typeName)

      //c.遍历list,创建Index对象,并放入Bulk.Builder对象
      list.foreach { case (docId, data) =>

        //给每一条数据创建Index对象
        val index: Index = new Index.Builder(data)
          .id(docId)
          .build()

        //将index放入builder对象中
        builder.addAction(index)
      }

      //d.Bulk.Builder创建Bulk
      val bulk: Bulk = builder.build()

      //e.执行写入数据操作
      client.execute(bulk)

      //f.释放连接
      close(client)

    }

  }

}

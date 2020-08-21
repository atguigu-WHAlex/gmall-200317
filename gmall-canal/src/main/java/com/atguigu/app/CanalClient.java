package com.atguigu.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstants;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;

public class CanalClient {

    public static void main(String[] args) {

        //获取Canal连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop102", 11111),
                "example",
                "",
                "");

        while (true) {

            //连接
            canalConnector.connect();
            //订阅监控的表
            canalConnector.subscribe("gmall200317.*");

            //抓取数据
            Message message = canalConnector.get(100);

            //判断当前是否抓取到数据
            if (message.getEntries().size() <= 0) {
                System.out.println("当前抓取没有数据,休息一下！！！");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {

                //1.获取message中的Entry集合并遍历
                for (CanalEntry.Entry entry : message.getEntries()) {

                    //2.获取entry中RowData类型的数据
                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {

                        try {
                            //3.获取Entry中的表名和数据
                            String tableName = entry.getHeader().getTableName();
                            ByteString storeValue = entry.getStoreValue();

                            //4.反序列化storeValue
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                            //5.获取事件类型
                            CanalEntry.EventType eventType = rowChange.getEventType();

                            //6.获取数据
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                            //7.处理数据
                            handler(tableName, eventType, rowDatasList);

                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    //处理数据,根据表名以及事件类型将数据发送至Kafka指定主题
    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {

        //只需要order_info表中的新增数据
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {

            sendToKafka(rowDatasList, GmallConstants.GMALL_TOPIC_ORDER_INFO);

            //只需要order_detail中的新增数据
        } else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {

            sendToKafka(rowDatasList, GmallConstants.GMALL_TOPIC_ORDER_DETAIL);

            //用户信息表,需要新增及变化数据
        } else if ("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType))) {

            sendToKafka(rowDatasList, GmallConstants.GMALL_TOPIC_USER_INFO);
        }

    }

    private static void sendToKafka(List<CanalEntry.RowData> rowDatasList, String topic) {
        //遍历行集
        for (CanalEntry.RowData rowData : rowDatasList) {
            //创建一个JSON对象用于存放一行数据
            JSONObject jsonObject = new JSONObject();
            //遍历修改之后的列集
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                jsonObject.put(column.getName(), column.getValue());
            }
            //打印单行数据并写入Kafka
//            try {
//                Thread.sleep(new Random().nextInt(5 * 1000));
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
            System.out.println(jsonObject.toString());
            MyKafkaSender.send(topic, jsonObject.toString());
        }
    }

}

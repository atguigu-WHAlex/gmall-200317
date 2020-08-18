package com.atguigu;

import com.atguigu.constants.GmallConstants;
import com.atguigu.utils.MyKafkaSender;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Test {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>(GmallConstants.GMALL_TOPIC_ORDER_INFO,
                    i % 2,
                    i + "",
                    "atguigu-" + i));
        }

        producer.close();

    }

}

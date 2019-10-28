package com.vilin.kafka.customer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Customer {

    public static void main(String[] args){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","192.168.193.128:9092");
        //key反序列化器：
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        //value反序列化器：
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        //定义消费者群组
        properties.setProperty("group.id", "1000");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Collections.singletonList("my-topic"));

        //同步提交偏移量
        //kafkaConsumer.commitSync();
        //异步提交偏移量
        //kafkaConsumer.commitAsync();

        while(true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.of(500, ChronoUnit.MILLIS));
            for(ConsumerRecord<String, String> context : records){
                System.out.println("消息所在分区：" + context.partition() + "-消息的偏移量：" + context.offset());
                System.out.println("消息key：" + context.key() + "消息value：" + context.value());

            }
        }
    }
}

package com.vilin.kafka.producter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producter {

    public static void main(String[] args){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","192.168.193.128:9092");
        //key序列化器：
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        //value序列化器：
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("my-topic","test.key","hello kafka");
        kafkaProducer.send(record);
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}

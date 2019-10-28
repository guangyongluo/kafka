package com.vilin.kafka.producter;

import com.vilin.kafka.partitioner.MyPartitioner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producter {

    public static void main(String[] args){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.193.128:9092");
        //key序列化器：
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //value序列化器：
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //自定义分区器
        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
        ProducerRecord<String, String> record1 = new ProducerRecord<String, String>("my-topic", null,"hello kafka");
        ProducerRecord<String, String> record2 = new ProducerRecord<String, String>("my-topic", "key","hello kafka");
        kafkaProducer.send(record1);
        kafkaProducer.send(record2);
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}

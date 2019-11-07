package com.vilin.kafka.chapter01;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class Producter {

    private Properties kafkaProps = new Properties();

    private KafkaProducer<String, String> producer;

    public Producter(){
        kafkaProps.put("bootstrap.servers", "192.168.193.128:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(kafkaProps);
    }

    public void  simplySend(){
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("CustomerCountry", "Precision Products", "France");
        try{
            producer.send(record);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void syncSend(){
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("CustomerCountry", "Precision Products", "France");
        try{
            producer.send(record).get();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private class DemoProducerCallBack implements Callback {
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if(e != null){
                e.printStackTrace();
            }
        }
    }

    public void AsyncSend(){
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("CustomerCountry", "Precision Products", "France");
        producer.send(record, new DemoProducerCallBack());
    }

    public static void main(String[] args){
        Producter producter = new Producter();
        producter.simplySend();
    }
}

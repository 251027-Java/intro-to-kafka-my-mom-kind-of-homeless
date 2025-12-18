package com.example.kafkamessager;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaService {
    // Fields
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    // Constructor

    // Methods

    // Producer
    public void sendMessage( String message ){
        System.out.println("Sending message to Kafka: " + message );
        kafkaTemplate.send("message-topic", message);
    }

    public void sendMessage( String key, String message ){
        System.out.println("Produced with key [" + key + "]: " + message );
        kafkaTemplate.send("message-topic", key, message);
    }


    // Consumer
//    @KafkaListener(topics = "message-topic", groupId = "group_id") // what are we listening for?
//    public void consume(String message){ // when a new message is produced, it triggers our mehtod with that message
//        System.out.println("Consumed message: " + message);
//    }

    @KafkaListener(topics = "message-topic", groupId = "group_id") // what are we listening for?
    public void consume(ConsumerRecord<String, String> record){ // when a new message is produced, it triggers our mehtod with that message
        String key = record.key();
        String message = record.value();

        System.out.println("Key: [" + key + "] Message: " + message);

        if ("user".equals(key)){
            System.out.println("USER: " + message );
        }
    }
}
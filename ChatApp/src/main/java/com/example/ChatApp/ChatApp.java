package com.example.kafkachat;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.Executors;

public class kafkachat {

    private static final String TOPIC = "global-chat";
    private static final String BOOTSTRAP = "localhost:9092";

    public static void main(String[] args) {
        String username = args.length > 0 ? args[0] : UUID.randomUUID().toString();

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps(username));

        consumer.subscribe(List.of(TOPIC));

        // Consumer thread
        Executors.newSingleThreadExecutor().submit(() -> {
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : records) {
                    if (!r.key().equals(username)) {
                        System.out.println(r.value());
                    }
                }
            }
        });

        // Producer loop
        Scanner scanner = new Scanner(System.in);
        while (true) {
            String msg = scanner.nextLine();
            producer.send(new ProducerRecord<>(
                    TOPIC,
                    username,
                    username + ": " + msg
            ));
        }
    }

    private static Properties producerProps() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return p;
    }

    private static Properties consumerProps(String user) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "chat-" + user);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return p;
    }
}

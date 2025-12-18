package com.revature.lab;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class ChatApp {

    private static final String TOPIC = "global-chat";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        // Prompt user for their username
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter your username: ");
        String username = scanner.nextLine();
        
        // Initialize Kafka Producer settings
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        // Initialize Kafka Consumer settings
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "chat-group-" + UUID.randomUUID().toString());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(TOPIC));

        // Start a Thread for the Consumer (Polling loop)
        Thread consumerThread = new Thread(() -> {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        String sender = record.key();
                        String message = record.value();
                        
                        // Display messages from other users
                        if (!sender.equals(username)) {
                            System.out.println("\n[" + sender + "]: " + message);
                            System.out.print("You: ");
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("Consumer error: " + e.getMessage());
            } finally {
                consumer.close();
            }
        });
        
        consumerThread.setDaemon(true);
        consumerThread.start();

        // Main loop reading Scanner(System.in) and sending messages
        System.out.println("\n=== Welcome to Global Chat ===");
        System.out.println("Type your messages and press Enter to send.");
        System.out.println("Type 'exit' to quit.\n");

        try {
            while (true) {
                System.out.print("You: ");
                String message = scanner.nextLine();
                
                if ("exit".equalsIgnoreCase(message.trim())) {
                    break;
                }
                
                if (!message.trim().isEmpty()) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, username, message);
                    producer.send(record);
                }
            }
        } catch (Exception e) {
            System.err.println("Producer error: " + e.getMessage());
        } finally {
            producer.close();
            scanner.close();
            System.out.println("Chat session ended. Goodbye!");
        }
    }
}

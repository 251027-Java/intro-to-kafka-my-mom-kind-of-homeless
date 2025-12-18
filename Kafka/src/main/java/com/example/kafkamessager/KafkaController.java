package com.example.kafkamessager;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaController {
    // Fields
    @Autowired
    private KafkaService service;

    // Constructor

    // Methods
    @PostMapping("/send")
    public String sendMessage(@RequestParam("message") String message){
        service.sendMessage(message);
        return "message sent!";
    }

    @PostMapping("/keyed")
    public String keyedMessage(@RequestParam("key") String key,
                               @RequestParam("message") String message){
        service.sendMessage(key, message);

        return "keyed message produced!";
    }
}










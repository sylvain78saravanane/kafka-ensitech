package com.learn.kafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;

@Component
public class MessageProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;


    public void sendMessage(String topic, String message) {
        kafkaTemplate.send(topic, message);
    }

}
package com.learn.kafka.consumer;


import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class MessageConsumer {

    private static Logger log = LoggerFactory.getLogger(MessageConsumer.class);

    @KafkaListener(topics = "${spring.kafka.consumer.topic-name}", groupId = "${spring.kafka.consumer.group-id}")
    public void send(String message) {
        log.info("Message receive : {}", message);
    }

}
package com.learn.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.client.RestTemplate;

@Configuration
@EnableScheduling
public class AppConfig {
    @Value("${exchange-rate.kafka.topic}")
    private String topicName;

    /**
     * Configuration du RestTemplate pour les appels API externes
     */
    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    /**
     * Configuration du topic Kafka
     */
    @Bean
    public NewTopic exchangeRatesTopic() {
        return TopicBuilder.name(topicName)
                .partitions(3)
                .replicas(1)
                .build();
    }


}

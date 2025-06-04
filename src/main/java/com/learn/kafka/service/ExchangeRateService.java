package com.learn.kafka.service;

import com.learn.kafka.model.ExchangeRateData;
import com.learn.kafka.model.ExternalApiResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDateTime;
import java.util.Optional;

@Service
@RequiredArgsConstructor
@Slf4j
public class ExchangeRateService {
    private final KafkaTemplate<String, ExchangeRateData> kafkaTemplate;

    private final RestTemplate restTemplate;

    private final ElasticsearchService elasticsearchService;

    @Value("${exchange-rate.api.url}")
    private String apiUrl;

    @Value("${exchange-rate.kafka.topic}")
    private String kafkaTopic;

    /**
     * Récupère les taux de change pour une devise de base donnée
     */
    public Optional<ExchangeRateData> fetchExchangeRates(String baseCurrency) {
        try {
            log.info("Fetching exchange rates for base currency: {}", baseCurrency);

            String url = apiUrl + "/" + baseCurrency;
            ExternalApiResponse response = restTemplate.getForObject(url, ExternalApiResponse.class);

            if (response != null && response.getRates() != null) {
                ExchangeRateData exchangeRateData = convertToExchangeRateData(response);

                // Publier sur Kafka
                publishToKafka(exchangeRateData);

                // Sauvegarder dans Elasticsearch
                elasticsearchService.saveExchangeRate(exchangeRateData);

                log.info("Successfully processed exchange rates for {}", baseCurrency);
                return Optional.of(exchangeRateData);
            }

            log.warn("No exchange rate data received for currency: {}", baseCurrency);
            return Optional.empty();

        } catch (Exception e) {
            log.error("Error fetching exchange rates for currency {}: {}", baseCurrency, e.getMessage(), e);
            return Optional.empty();
        }
    }

    /**
     * Convertit la réponse de l'API externe en modèle interne
     */
    private ExchangeRateData convertToExchangeRateData(ExternalApiResponse response) {
        ExchangeRateData data = ExchangeRateData.builder()
                .baseCurrency(response.getBase())
                .timestamp(LocalDateTime.now())
                .dateUnix(response.getTimeLastUpdated())
                .rates(response.getRates())
                .source("API")
                .provider(response.getProvider() != null ? response.getProvider() : "exchangerate-api.com")
                .build();

        data.generateId();
        return data;
    }

    /**
     * Publie les données sur Kafka
     */
    private void publishToKafka(ExchangeRateData exchangeRateData) {
        try {
            kafkaTemplate.send(kafkaTopic, exchangeRateData.getBaseCurrency(), exchangeRateData);
            log.debug("Published exchange rate data to Kafka topic: {}", kafkaTopic);
        } catch (Exception e) {
            log.error("Error publishing to Kafka: {}", e.getMessage(), e);
        }
    }

    /**
     * Récupère les taux de change depuis Elasticsearch
     */
    public Optional<ExchangeRateData> getLatestExchangeRates(String baseCurrency) {
        return elasticsearchService.findLatestByBaseCurrency(baseCurrency);
    }

}

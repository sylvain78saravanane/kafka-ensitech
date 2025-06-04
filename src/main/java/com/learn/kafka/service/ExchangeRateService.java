package com.learn.kafka.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafka.model.ExchangeRateData;
import com.learn.kafka.model.ExternalApiResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

@Service
@RequiredArgsConstructor
@Slf4j
public class ExchangeRateService {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RestTemplate restTemplate;
    private final ElasticsearchService elasticsearchService;
    // CORRIGÉ : Injection de ObjectMapper configuré au lieu de new ObjectMapper()
    private final ObjectMapper objectMapper;

    @Value("${exchange-rate.api.url}")
    private String apiUrl;

    @Value("${exchange-rate.kafka.topic}")
    private String kafkaTopic;

    /**
     * Récupère les taux de change pour une devise de base donnée
     */
    public Optional<ExchangeRateData> fetchExchangeRates(String baseCurrency) {
        try {
            log.info("🔄 DÉBUT fetchExchangeRates pour: {}", baseCurrency);

            String url = apiUrl + "/" + baseCurrency;
            log.info("📡 Appel API: {}", url);

            ExternalApiResponse response = restTemplate.getForObject(url, ExternalApiResponse.class);

            if (response != null && response.getRates() != null) {
                log.info("✅ API Response reçue - Devise: {}, Nombre de taux: {}",
                        response.getBase(), response.getRates().size());

                ExchangeRateData exchangeRateData = convertToExchangeRateData(response);
                log.info("🔧 Data convertie - ID: {}", exchangeRateData.getId());

                // Publier sur Kafka
                log.info("📤 TENTATIVE envoi Kafka...");
                publishToKafka(exchangeRateData);

                // Sauvegarder dans Elasticsearch
                log.info("💾 TENTATIVE sauvegarde Elasticsearch...");
                elasticsearchService.saveExchangeRate(exchangeRateData);

                log.info("✅ fetchExchangeRates TERMINÉ avec succès pour {}", baseCurrency);
                return Optional.of(exchangeRateData);
            }

            log.warn("⚠️ Pas de données reçues de l'API pour: {}", baseCurrency);
            return Optional.empty();

        } catch (Exception e) {
            log.error("❌ ERREUR dans fetchExchangeRates pour {}: {}", baseCurrency, e.getMessage(), e);
            return Optional.empty();
        }
    }

    /**
     * Convertit la réponse de l'API externe en modèle interne
     */
    private ExchangeRateData convertToExchangeRateData(ExternalApiResponse response) {
        ExchangeRateData data = ExchangeRateData.builder()
                .baseCurrency(response.getBase())
                // CORRIGÉ : Zone horaire valide
                .timestamp(ZonedDateTime.now(ZoneId.of("Europe/Paris")))
                .dateUnix(response.getTimeLastUpdated())
                .rates(response.getRates())
                .source("API")
                .provider(response.getProvider() != null ? response.getProvider() : "exchangerate-api.com")
                .build();

        data.generateId();
        log.info("🆔 ID généré: {}", data.getId());
        return data;
    }

    /**
     * Publie les données sur Kafka (converti en JSON String)
     */
    private void publishToKafka(ExchangeRateData exchangeRateData) {
        try {
            log.info("🚀 DÉBUT publishToKafka");
            log.info("📋 Topic: {}", kafkaTopic);
            log.info("🔑 Key: {}", exchangeRateData.getBaseCurrency());

            // VÉRIFICATION : kafkaTemplate est-il null ?
            if (kafkaTemplate == null) {
                log.error("❌ ERREUR CRITIQUE: kafkaTemplate est NULL !");
                return;
            }

            String jsonMessage = objectMapper.writeValueAsString(exchangeRateData);
            log.info("📄 JSON créé - Taille: {} caractères", jsonMessage.length());
            log.info("📝 JSON preview: {}", jsonMessage.substring(0, Math.min(200, jsonMessage.length())));

            log.info("📤 Envoi vers Kafka...");
            kafkaTemplate.send(kafkaTopic, exchangeRateData.getBaseCurrency(), jsonMessage);

            // CORRIGÉ : INFO au lieu de DEBUG
            log.info("✅ Message envoyé avec succès sur Kafka topic: {}", kafkaTopic);

        } catch (Exception e) {
            log.error("❌ ERREUR Kafka: {}", e.getMessage(), e);
            e.printStackTrace(); // Pour voir la stack trace complète
        }
    }

    /**
     * Récupère les taux de change depuis Elasticsearch
     */
    public Optional<ExchangeRateData> getLatestExchangeRates(String baseCurrency) {
        return elasticsearchService.findLatestByBaseCurrency(baseCurrency);
    }
}
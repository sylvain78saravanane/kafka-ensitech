package com.learn.kafka.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learn.kafka.model.ExchangeRateData;
import com.learn.kafka.model.ExternalApiResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
@ExtendWith(MockitoExtension.class)
@DisplayName("Test du Rest Service")
public class ExchangeRateServiceTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;
    @Mock
    private RestTemplate restTemplate;
    @Mock
    private ElasticsearchService elasticsearchService;
    @Mock
    private ObjectMapper objectMapper;
    private ExchangeRateService exchangeRateService;
    private static final String TEST_API_URL = "https://api.test.com/v4/latest";
    private static final String TEST_KAFKA_TOPIC = "test-exchange-rates";
    @BeforeEach
    void setUp() {
        exchangeRateService = new ExchangeRateService(
                kafkaTemplate,
                restTemplate,
                elasticsearchService,
                objectMapper
        );

        ReflectionTestUtils.setField(exchangeRateService, "apiUrl", TEST_API_URL);
        ReflectionTestUtils.setField(exchangeRateService,"kafkaTopic", TEST_KAFKA_TOPIC);
    }
    @Test
    @DisplayName("CAS DE SUCCÈS - Doit récupérer, publier sur Kafka et sauvegarder dans Elasticsearch")
    void shouldSuccessfullyFetchPublishAndSaveExchangeRates() throws Exception {

        String baseCurrency = "USD";

        ExternalApiResponse mockApiResponse = new ExternalApiResponse();
        mockApiResponse.setBase("USD");
        mockApiResponse.setProvider("exchangerate-api.com");
        mockApiResponse.setTimeLastUpdated(1735995600L); // timestamp Unix

        Map<String, BigDecimal> rates = new HashMap<>();
        rates.put("EUR", new BigDecimal("0.85"));
        rates.put("GBP", new BigDecimal("0.75"));
        rates.put("JPY", new BigDecimal("110.25"));
        mockApiResponse.setRates(rates);


        String expectedJson = "{\"id\":\"USD_2025-06-04_14-30-00\",\"baseCurrency\":\"USD\",\"rates\":{\"EUR\":0.85,\"GBP\":0.75,\"JPY\":110.25}}";

        when(restTemplate.getForObject(TEST_API_URL + "/USD", ExternalApiResponse.class))
                .thenReturn(mockApiResponse);
        when(objectMapper.writeValueAsString(any(ExchangeRateData.class)))
                .thenReturn(expectedJson);

        Optional<ExchangeRateData> result = exchangeRateService.fetchExchangeRates(baseCurrency);


        assertThat(result).isPresent();
        assertThat(result.get().getBaseCurrency()).isEqualTo("USD");
        assertThat(result.get().getRates()).hasSize(3);
        assertThat(result.get().getRates().get("EUR")).isEqualTo(new BigDecimal("0.85"));
        assertThat(result.get().getProvider()).isEqualTo("exchangerate-api.com");
        assertThat(result.get().getSource()).isEqualTo("API");
        assertThat(result.get().getId()).isNotNull();

        verify(restTemplate, times(1))
                .getForObject(TEST_API_URL + "/USD", ExternalApiResponse.class);

        // Vérifier la publication sur Kafka avec ArgumentCaptor(Mockito)
        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);

        verify(kafkaTemplate, times(1))
                .send(topicCaptor.capture(), keyCaptor.capture(), messageCaptor.capture());

        assertThat(topicCaptor.getValue()).isEqualTo(TEST_KAFKA_TOPIC);
        assertThat(keyCaptor.getValue()).isEqualTo("USD");
        assertThat(messageCaptor.getValue()).isEqualTo(expectedJson);

        // Vérifier la sauvegarde dans Elasticsearch
        ArgumentCaptor<ExchangeRateData> dataCaptor = ArgumentCaptor.forClass(ExchangeRateData.class);
        verify(elasticsearchService, times(1))
                .saveExchangeRate(dataCaptor.capture());

        ExchangeRateData savedData = dataCaptor.getValue();
        assertThat(savedData.getBaseCurrency()).isEqualTo("USD");
        assertThat(savedData.getRates()).hasSize(3);

        // Vérifier la sérialisation JSON
        verify(objectMapper, times(1))
                .writeValueAsString(any(ExchangeRateData.class));
    }
}

package com.learn.kafka.controller;

import com.learn.kafka.model.ExchangeRateData;
import com.learn.kafka.service.ElasticsearchService;
import com.learn.kafka.service.ExchangeRateService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("api/exchange-rates")
@RequiredArgsConstructor
@Slf4j
public class ExchangeRateController {

    private final ExchangeRateService exchangeRateService;
    private final ElasticsearchService elasticsearchService;

    /**
     * Endpoint pour get les tx de change actuels
     * avec GET : /api/exchange-rates/{baseCurrency}
     */
    @GetMapping("/{baseCurrency}")
    public ResponseEntity<ExchangeRateData> getCurrentRates(@Valid @PathVariable String baseCurrency){
        log.info("Request for current exchange rates with base currency: {}", baseCurrency);

        Optional<ExchangeRateData> exchangeRates = exchangeRateService.fetchExchangeRates(baseCurrency.toUpperCase());

        return exchangeRates
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Endpoint pour récupérer les derniers taux depuis Elasticsearch
     * GET /api/exchange-rates/{baseCurrency}/latest
     */
    @GetMapping("/{baseCurrency}/latest")
    public ResponseEntity<ExchangeRateData> getLatestRates(@Valid @PathVariable String baseCurrency) {
        log.info("Request for latest stored exchange rates with base currency: {}", baseCurrency);

        Optional<ExchangeRateData> exchangeRates = exchangeRateService.getLatestExchangeRates(baseCurrency.toUpperCase());

        return exchangeRates
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Endpoint pour récupérer un taux spécifique
     * GET /api/exchange-rates/{baseCurrency}/rate/{targetCurrency}
     */
    @GetMapping("/{baseCurrency}/rate/{targetCurrency}")
    public ResponseEntity<Map<String, Object>> getSpecificRate(
            @PathVariable String baseCurrency,
            @PathVariable String targetCurrency) {

        log.info("Request for specific rate: {} to {}", baseCurrency, targetCurrency);

        Optional<ExchangeRateData> exchangeRates = exchangeRateService.getLatestExchangeRates(baseCurrency.toUpperCase());

        if (exchangeRates.isPresent()) {
            BigDecimal rate = exchangeRates.get().getRateFor(targetCurrency.toUpperCase());
            if (rate != null) {
                Map<String, Object> response = Map.of(
                        "baseCurrency", baseCurrency.toUpperCase(),
                        "targetCurrency", targetCurrency.toUpperCase(),
                        "rate", rate,
                        "timestamp", exchangeRates.get().getTimestamp()
                );
                return ResponseEntity.ok(response);
            }
        }

        return ResponseEntity.notFound().build();
    }

    /**
     * Endpoint pour récupérer l'historique des taux
     * GET /api/exchange-rates/{baseCurrency}/history
     */
    @GetMapping("/{baseCurrency}/history")
    public ResponseEntity<List<ExchangeRateData>> getHistoricalRates(
            @PathVariable String baseCurrency,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to) {

        log.info("Request for historical rates for {}: from {} to {}", baseCurrency, from, to);

        if (from == null) {
            from = LocalDateTime.now().minusDays(7); // Par défaut, dernière semaine
        }
        if (to == null) {
            to = LocalDateTime.now();
        }

        List<ExchangeRateData> historicalRates = elasticsearchService.findByBaseCurrencyAndDateRange(
                baseCurrency.toUpperCase(), from, to);

        return ResponseEntity.ok(historicalRates);
    }

    /**
     * Endpoint pour récupérer tous les taux récents
     * GET /api/exchange-rates/recent
     */
    @GetMapping("/recent")
    public ResponseEntity<List<ExchangeRateData>> getRecentRates() {
        log.info("Request for all recent exchange rates");

        List<ExchangeRateData> recentRates = elasticsearchService.findRecentExchangesRates();
        return ResponseEntity.ok(recentRates);
    }

    /**
     * Endpoint pour forcer la mise à jour des taux
     * POST /api/exchange-rates/{baseCurrency}/refresh
     */
    @PostMapping("/{baseCurrency}/refresh")
    public ResponseEntity<ExchangeRateData> refreshRates(@PathVariable String baseCurrency) {
        log.info("Manual refresh requested for currency: {}", baseCurrency);

        Optional<ExchangeRateData> exchangeRates = exchangeRateService.fetchExchangeRates(baseCurrency.toUpperCase());

        return exchangeRates
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.badRequest().build());
    }

    /**
     * Endpoint de santé pour vérifier le statut du service
     * GET /api/exchange-rates/health
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getHealth() {
        long totalDocuments = elasticsearchService.countAll();

        Map<String, Object> health = Map.of(
                "status", "UP",
                "totalStoredRates", totalDocuments,
                "timestamp", LocalDateTime.now()
        );

        return ResponseEntity.ok(health);
    }
}

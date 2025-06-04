package com.learn.kafka.scheduler;

import com.learn.kafka.service.ExchangeRateService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class ScheduledTasks {

    private final ExchangeRateService exchangeRateService;

    @Value("${exchange-rate.api.default-base-currency}")
    private String defaultBaseCurrency;

    // Liste des principales devises à surveiller
    private final List<String> mainCurrencies = List.of("USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD");

    /**
     * Tâche programmée pour récupérer les taux de change toutes les minutes
     */
    @Scheduled(fixedRateString = "${exchange-rate.scheduler.fixed-rate}",
            initialDelayString = "${exchange-rate.scheduler.initial-delay}")
    public void fetchExchangeRatesScheduled() {
        log.info("Starting scheduled exchange rate fetch");

        // Récupération pour la devise de base par défaut
        exchangeRateService.fetchExchangeRates(defaultBaseCurrency);

        // Récupération pour les autres principales devises
        mainCurrencies.stream()
                .filter(currency -> !currency.equals(defaultBaseCurrency))
                .forEach(currency -> {
                    try {
                        Thread.sleep(100); // Petite pause pour éviter de surcharger l'API
                        exchangeRateService.fetchExchangeRates(currency);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.error("Thread interrupted while fetching rates for {}", currency);
                    }
                });

        log.info("Completed scheduled exchange rate fetch");
    }

    /**
     * Tâche de nettoyage quotidien des anciennes données
     */
    @Scheduled(cron = "0 0 2 * * *") // Tous les jours à 2h du matin
    public void dailyCleanup() {
        log.info("Starting daily cleanup task");
        log.info("Daily cleanup completed");
    }

    /**
     * Tâche de monitoring hebdomadaire
     */
    @Scheduled(cron = "0 0 8 * * MON") // Tous les lundis à 8h
    public void weeklyMonitoring() {
        log.info("Starting weekly monitoring task");
        log.info("Weekly monitoring completed");
    }
}

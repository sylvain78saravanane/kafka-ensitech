package com.learn.kafka.service;

import com.learn.kafka.model.ExchangeRateData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHits;
import org.springframework.data.elasticsearch.core.query.Criteria;
import org.springframework.data.elasticsearch.core.query.CriteriaQuery;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class ElasticsearchService {
    private final ElasticsearchOperations elasticsearchOperations;

    public void saveExchangeRate(ExchangeRateData exchangeRateData) {
        try {
            elasticsearchOperations.save(exchangeRateData);
            log.info("Saved exchange rate data to Elasticsearch: ID={}, BaseCurrency={}, RatesCount={}",
                    exchangeRateData.getId(),
                    exchangeRateData.getBaseCurrency(),
                    exchangeRateData.getRates() != null ? exchangeRateData.getRates().size() : 0);
        } catch (Exception e) {
            log.error("Error saving to Elasticsearch: {}", e.getMessage(), e);
        }
    }

    public Optional<ExchangeRateData> findLatestByBaseCurrency(String baseCurrency) {
        try {
            Criteria criteria = new Criteria("baseCurrency").is(baseCurrency);
            CriteriaQuery query = new CriteriaQuery(criteria)
                    .setPageable(PageRequest.of(0, 1, Sort.by(Sort.Direction.DESC, "timestamp")));

            SearchHits<ExchangeRateData> searchHits = elasticsearchOperations.search(query, ExchangeRateData.class);

            return searchHits.getSearchHits().stream()
                    .findFirst()
                    .map(SearchHit::getContent);

        } catch (Exception e) {
            log.error("Error finding latest exchange rates for {}: {}", baseCurrency, e.getMessage(), e);
            return Optional.empty();
        }
    }

    /**
     * Trouve les taux de change dans une plage de dates
     */
    public List<ExchangeRateData> findByBaseCurrencyAndDateRange(
            String baseCurrency,
            LocalDateTime from,
            LocalDateTime to)
    {
        try {
            Criteria criteria = new Criteria("baseCurrency").is(baseCurrency)
                    .and(new Criteria("timestamp").between(from, to));

            CriteriaQuery query = new CriteriaQuery(criteria)
                    .setPageable(PageRequest.of(0, 100, Sort.by(Sort.Direction.DESC, "timestamp")));

            SearchHits<ExchangeRateData> searchHits = elasticsearchOperations.search(query, ExchangeRateData.class);

            return searchHits.getSearchHits().stream()
                    .map(SearchHit::getContent)
                    .collect(Collectors.toList());

        } catch (Exception e) {
            log.error("Error finding exchange rates by date range: {}", e.getMessage(), e);
            return List.of();
        }
    }

    /**
     * Trouve tous les taux de change récents (dernières 24h)
     */
    public List<ExchangeRateData> findRecentExchangesRates(){
        LocalDateTime yesterday = LocalDateTime.now().minusDays(1);

        try {
            Criteria criteria = new Criteria("timestamp").greaterThan(yesterday);
            CriteriaQuery query = new CriteriaQuery(criteria)
                    .setPageable(PageRequest.of(0, 50, Sort.by(Sort.Direction.DESC, "timestamp")));

            SearchHits<ExchangeRateData> searchHits = elasticsearchOperations.search(query, ExchangeRateData.class);

            return searchHits.getSearchHits().stream()
                    .map(SearchHit::getContent)
                    .collect(Collectors.toList());

        } catch (Exception e) {
            log.error("Error finding recent exchange rates: {}", e.getMessage(), e);
            return List.of();
        }
    }

    /**
     * Compte le nombre total de documents
     */
    public long countAll() {
        try {
            return elasticsearchOperations.count(new CriteriaQuery(new Criteria()), ExchangeRateData.class);
        } catch (Exception e) {
            log.error("Error counting documents: {}", e.getMessage(), e);
            return 0;
        }
    }
}
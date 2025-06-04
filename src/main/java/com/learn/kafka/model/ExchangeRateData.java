package com.learn.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(indexName = "exchange-rates")
public class ExchangeRateData {
    @Id
    private String id;

    @Field(type = FieldType.Keyword)
    private String baseCurrency;

    @Field(type = FieldType.Date)
    private ZonedDateTime timestamp;

    @Field(type = FieldType.Long)
    private Long dateUnix;

    @Field(type = FieldType.Object)
    private Map<String, BigDecimal> rates;

    @Field(type = FieldType.Keyword)
    private String source;

    @Field(type = FieldType.Text)
    private String provider;

    // Méthode utilitaire pour générer un ID unique
    public void generateId() {
        this.id = baseCurrency + "_" + timestamp.toString().replace(":", "-");
    }

    // Méthode pour obtenir le taux d'une devise spécifique
    public BigDecimal getRateFor(String currency) {
        return rates != null ? rates.get(currency.toUpperCase()) : null;
    }

}

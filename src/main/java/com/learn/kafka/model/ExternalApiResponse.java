package com.learn.kafka.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.math.BigDecimal;
import java.util.Map;

@Data
public class ExternalApiResponse {
    @JsonProperty("provider")
    private String provider;

    @JsonProperty("warning")
    private String warning;

    @JsonProperty("terms")
    private String terms;

    @JsonProperty("base")
    private String base;

    @JsonProperty("date")
    private String date;

    @JsonProperty("time_last_updated")
    private Long timeLastUpdated;

    @JsonProperty("rates")
    private Map<String, BigDecimal> rates;
}

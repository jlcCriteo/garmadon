package com.criteo.hadoop.garmadon.opensearch.configurations;

import com.criteo.hadoop.garmadon.reader.configurations.KafkaConfiguration;
import com.criteo.hadoop.garmadon.reader.configurations.PrometheusConfiguration;

public class OpenSearchReaderConfiguration {
    private OpensearchConfiguration opensearch;
    private KafkaConfiguration kafka;
    private PrometheusConfiguration prometheus;

    public OpensearchConfiguration getOpensearch() {
        return opensearch;
    }

    public void setOpensearch(OpensearchConfiguration opensearch) {
        this.opensearch = opensearch;
    }

    public KafkaConfiguration getKafka() {
        return kafka;
    }

    public void setKafka(KafkaConfiguration kafka) {
        this.kafka = kafka;
    }

    public PrometheusConfiguration getPrometheus() {
        return prometheus;
    }

    public void setPrometheus(PrometheusConfiguration prometheus) {
        this.prometheus = prometheus;
    }
}

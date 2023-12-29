package com.criteo.hadoop.garmadon.opensearch;

import com.criteo.hadoop.garmadon.elasticsearch.EventHelper;
import com.criteo.hadoop.garmadon.elasticsearch.cache.ElasticSearchCacheManager;
import com.criteo.hadoop.garmadon.event.proto.JVMStatisticsEventsProtos;
import com.criteo.hadoop.garmadon.opensearch.configurations.OpensearchConfiguration;
import com.criteo.hadoop.garmadon.opensearch.configurations.OpenSearchReaderConfiguration;
import com.criteo.hadoop.garmadon.reader.CommittableOffset;
import com.criteo.hadoop.garmadon.reader.GarmadonMessage;
import com.criteo.hadoop.garmadon.reader.GarmadonMessageFilter;
import com.criteo.hadoop.garmadon.reader.GarmadonReader;
import com.criteo.hadoop.garmadon.reader.configurations.KafkaConfiguration;
import com.criteo.hadoop.garmadon.reader.configurations.ReaderConfiguration;
import com.criteo.hadoop.garmadon.reader.metrics.PrometheusHttpConsumerMetrics;
import com.criteo.hadoop.garmadon.schema.serialization.GarmadonSerialization;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.ActionListener;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.client.Node;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.sniff.OpenSearchNodesSniffer;
import org.opensearch.client.sniff.SniffOnFailureListener;
import org.opensearch.client.sniff.Sniffer;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public final class OpenSearchReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenSearchReader.class);
    private static final int CONNECTION_TIMEOUT_MS = 10000;
    private static final int SOCKET_TIMEOUT_MS = 60000;
    private static final int NB_RETRIES = 10;
    private static final Format FORMATTER = new SimpleDateFormat("yyyy-MM-dd-HH");

    private final GarmadonReader reader;
    private final String indexPrefix;
    private final BulkProcessor bulkProcessor;
    private final PrometheusHttpConsumerMetrics prometheusHttpConsumerMetrics;
    private final ElasticSearchCacheManager elasticSearchCacheManager;

    OpenSearchReader(GarmadonReader.Builder builderReader,
                     BulkProcessor bulkProcessorMain,
                     String indexPrefix,
                     PrometheusHttpConsumerMetrics prometheusHttpConsumerMetrics,
                     ElasticSearchCacheManager elasticSearchCacheManager) {
        this.reader = builderReader
                .intercept(GarmadonMessageFilter.ANY.INSTANCE, this::writeToES)
                .build();

        this.bulkProcessor = bulkProcessorMain;

        this.indexPrefix = indexPrefix;
        this.prometheusHttpConsumerMetrics = prometheusHttpConsumerMetrics;
        this.elasticSearchCacheManager = elasticSearchCacheManager;
    }

    private CompletableFuture<Void> startReading() {
        return reader.startReading().whenComplete((v, ex) -> {
            if (ex != null) {
                LOGGER.error("Reading was stopped due to exception");
                ex.printStackTrace();
            } else {
                LOGGER.info("Done reading !");
            }
        });
    }

    private CompletableFuture<Void> stop() {
        return reader
                .stopReading()
                .whenComplete((vd, ex) -> {
                    try {
                        bulkProcessor.awaitClose(10L, TimeUnit.SECONDS);
                        prometheusHttpConsumerMetrics.terminate();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                });
    }

    void writeToES(GarmadonMessage msg) {
        String msgType = GarmadonSerialization.getTypeName(msg.getType());
        long timestampMillis = msg.getTimestamp();

        elasticSearchCacheManager.cacheEnrichableData(msg);

        if (GarmadonSerialization.TypeMarker.JVMSTATS_EVENT == msg.getType()) {
            Map<String, Object> jsonMap = msg.getHeaderMap(true);

            HashMap<String, Map<String, Object>> eventMaps = new HashMap<>();
            EventHelper.processJVMStatisticsData(msgType, (JVMStatisticsEventsProtos.JVMStatisticsData) msg.getBody(), eventMaps);

            for (Map<String, Object> eventMap : eventMaps.values()) {
                eventMap.putAll(jsonMap);
                addEventToBulkProcessor(eventMap, timestampMillis, msg.getCommittableOffset());
            }
        } else {
            Map<String, Object> eventMap = msg.toMap(true, true);

            addEventToBulkProcessor(eventMap, timestampMillis, msg.getCommittableOffset());
        }
    }

    private void addEventToBulkProcessor(Map<String, Object> eventMap, long timestampMillis, CommittableOffset committableOffset) {
        eventMap.remove("id"); // field only used as kafka key

        elasticSearchCacheManager.enrichEvent(eventMap);

        String dailyIndex = indexPrefix + "-" + FORMATTER.format(timestampMillis);
        IndexRequest req = new GarmadonIndexRequest(dailyIndex, committableOffset)
                .source(eventMap);
        bulkProcessor.add(req);
    }

    private static class LogFailureListener extends SniffOnFailureListener {
        LogFailureListener() {
            super();
        }

        @Override
        public void onFailure(Node node) {
            LOGGER.warn("Node failed: " + node.getHost().getHostName() + "-" + node.getHost().getPort());
            super.onFailure(node);
        }
    }

    private static GarmadonReader.Builder setUpKafkaReader(KafkaConfiguration kafka) {
        // setup kafka reader
        Properties props = new Properties();

        props.putAll(GarmadonReader.Builder.DEFAULT_KAFKA_PROPS);
        props.putAll(kafka.getSettings());

        return GarmadonReader.Builder
                .stream(new KafkaConsumer<>(props))
                .withSubscriptions(kafka.getTopics());
    }

    private static BulkProcessor setUpBulkProcessor(OpensearchConfiguration opensearchConf) {
        final HttpHost host = new HttpHost(opensearchConf.getHost(), opensearchConf.getPort(), opensearchConf.getScheme());
        final LogFailureListener sniffOnFailureListener = new LogFailureListener();

        RestClientBuilder restClientBuilder = RestClient.builder(host)
                .setFailureListener(sniffOnFailureListener)
                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                    .setConnectTimeout(CONNECTION_TIMEOUT_MS)
                    .setSocketTimeout(SOCKET_TIMEOUT_MS)
                    .setContentCompressionEnabled(true));

        if (opensearchConf.getUser() != null) {
            final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();

            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(opensearchConf.getUser(), opensearchConf.getPassword()));
            restClientBuilder
                    .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                            .setDefaultCredentialsProvider(credentialsProvider));
        }

        RestHighLevelClient highLevelClient = new RestHighLevelClient(restClientBuilder);

        OpenSearchNodesSniffer nodesSniffer = new OpenSearchNodesSniffer(
                highLevelClient.getLowLevelClient(),
                OpenSearchNodesSniffer.DEFAULT_SNIFF_REQUEST_TIMEOUT,
                OpenSearchNodesSniffer.Scheme.valueOf(opensearchConf.getScheme().toUpperCase()));

        Sniffer sniffer = Sniffer.builder(highLevelClient.getLowLevelClient()).setNodesSniffer(nodesSniffer).build();
        sniffOnFailureListener.setSniffer(sniffer);

        BiConsumer<BulkRequest, ActionListener<BulkResponse>> bulkConsumer =
                (request, bulkListener) -> highLevelClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);

        return BulkProcessor.builder(bulkConsumer, new OpenSearchBulkListener())
                .setBulkActions(opensearchConf.getBulkActions())
                .setBulkSize(new ByteSizeValue(opensearchConf.getBulkSizeMB(), ByteSizeUnit.MB))
                .setFlushInterval(TimeValue.timeValueSeconds(opensearchConf.getBulkFlushIntervalSec()))
                .setConcurrentRequests(opensearchConf.getBulkConcurrent())
                .setBackoffPolicy(
                        BackoffPolicy.exponentialBackoff(TimeValue.timeValueMillis(100), NB_RETRIES)
                )
                .build();
    }

    public static void main(String[] args) throws IOException {
        OpenSearchReaderConfiguration config = ReaderConfiguration.loadConfig(OpenSearchReaderConfiguration.class);

        GarmadonReader.Builder builderReader = setUpKafkaReader(config.getKafka());
        BulkProcessor bulkProcessorMain = setUpBulkProcessor(config.getOpensearch());

        OpenSearchReader reader = new OpenSearchReader(builderReader, bulkProcessorMain,
                config.getOpensearch().getIndexPrefix(),
                new PrometheusHttpConsumerMetrics(config.getPrometheus().getPort()),
                new ElasticSearchCacheManager());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> reader.stop().join()));

        reader.startReading().join();
    }
}

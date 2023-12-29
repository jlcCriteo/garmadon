package com.criteo.hadoop.garmadon.opensearch;

import com.criteo.hadoop.garmadon.reader.CommittableOffset;
import com.criteo.hadoop.garmadon.reader.GarmadonReader;
import com.criteo.hadoop.garmadon.reader.metrics.PrometheusHttpConsumerMetrics;
import io.prometheus.client.Counter;
import io.prometheus.client.Summary;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A reader that pushes events to elastic search
 */
public final class OpenSearchBulkListener implements BulkProcessor.Listener {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenSearchBulkListener.class);

    private static final Counter.Child numberOfEventInError = PrometheusHttpConsumerMetrics.GARMADON_READER_METRICS.labels("number_of_event_in_error",
            GarmadonReader.getHostname(),
            PrometheusHttpConsumerMetrics.RELEASE);
    private static final Counter.Child numberOfOffsetCommitError = PrometheusHttpConsumerMetrics.GARMADON_READER_METRICS.labels("number_of_offset_commit_error",
            GarmadonReader.getHostname(),
            PrometheusHttpConsumerMetrics.RELEASE);
    private static final Summary.Child latencyIndexingEvents = PrometheusHttpConsumerMetrics.LATENCY_INDEXING_TO_ES.labels("latency_indexing_events",
            GarmadonReader.getHostname(),
            PrometheusHttpConsumerMetrics.RELEASE);

    @Override
    public void beforeBulk(long executionId, BulkRequest request) {
        LOGGER.debug("Executing Bulk[{}] with {} requests of {} Bytes", executionId,
                request.numberOfActions(),
                request.estimatedSizeInBytes());
    }

    @Override
    public void afterBulk(long executionId, BulkRequest bulkReq, BulkResponse response) {
        if (response.hasFailures()) {
            LOGGER.error("Bulk[{}] executed with failures", executionId);
            for (BulkItemResponse item : response.getItems()) {
                if (item.isFailed()) {
                    LOGGER.error("Failed on {} due to {}", item.getId(), item.getFailureMessage());
                    numberOfEventInError.inc();
                }
            }
        } else {
            LOGGER.info("Successfully completed Bulk[{}] in {} ms", executionId, response.getTook().getMillis());
            latencyIndexingEvents.observe(response.getTook().getMillis());
        }

        GarmadonIndexRequest lastIdxReq = (GarmadonIndexRequest) bulkReq.requests().get(bulkReq.requests().size() - 1);
        CommittableOffset<String, byte[]> lastOffset = lastIdxReq.getCommittableOffset();
        lastOffset
                .commitAsync()
                .whenComplete((topicPartitionOffset, exception) -> {
                    if (exception != null) {
                        LOGGER.warn("Could not commit kafka offset {}|{}", lastOffset.getOffset(), lastOffset.getPartition());
                        numberOfOffsetCommitError.inc();
                    } else {
                        LOGGER.info("Committed kafka offset {}|{}", topicPartitionOffset.getOffset(), topicPartitionOffset.getPartition());
                    }
                });
    }

    @Override
    public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
        LOGGER.error("Failed to execute Bulk[{}]", executionId, failure);
        numberOfEventInError.inc(request.requests().size());
    }
}

package com.criteo.hadoop.garmadon.opensearch;

import com.criteo.hadoop.garmadon.reader.CommittableOffset;
import org.opensearch.action.index.IndexRequest;

public class GarmadonIndexRequest extends IndexRequest {
    final private CommittableOffset committableOffset;

    GarmadonIndexRequest(String index, CommittableOffset committableOffset) {
        super(index);
        this.committableOffset = committableOffset;
    }

    public CommittableOffset getCommittableOffset() {
        return this.committableOffset;
    }
}

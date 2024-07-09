package io.milvus.benchmark;

import io.milvus.benchmark.BenchmarkConfig;
import io.milvus.benchmark.QueryVectors;

public abstract class SearchRunner implements Runnable {
    protected BenchmarkConfig config;
    protected QueryVectors queryVectors;
    protected String failedReason;

    public SearchRunner(QueryVectors queryVectors, BenchmarkConfig config) {
        this.queryVectors = queryVectors;
        this.config = config;
    }

    public String failedReason() {
        return this.failedReason;
    }

    public abstract long executedReuests();
}

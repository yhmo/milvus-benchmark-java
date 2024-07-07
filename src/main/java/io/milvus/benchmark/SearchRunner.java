package io.milvus.benchmark;

import io.milvus.benchmark.BenchmarkConfig;
import io.milvus.benchmark.QueryVectors;

public abstract class SearchRunner implements Runnable {
    protected BenchmarkConfig config;
    protected QueryVectors queryVectors;

    public SearchRunner(QueryVectors queryVectors, BenchmarkConfig config) {
        this.queryVectors = queryVectors;
        this.config = config;
    }

    public abstract long executedReuests();
}

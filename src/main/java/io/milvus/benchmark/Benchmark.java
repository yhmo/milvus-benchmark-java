package io.milvus.benchmark;

import io.milvus.ingestion.Ingestion;

public abstract class Benchmark {
    protected BenchmarkConfig config;
    protected BenchmarkResult result = new BenchmarkResult();
    protected Ingestion ingestion;
    protected GroundTruth groundTruth;
    protected QueryVectors queryVectors;

    public Benchmark(BenchmarkConfig config) {
        this.config = config;
    }

    protected abstract void recallAndLatencyTest();
    protected abstract void qpsTest();

    public BenchmarkResult run() {
        if (ingestion == null) {
            result.failedReason = "Ingestion is null";
            return result;
        }
        if (!ingestion.run()) {
            result.failedReason = "Failed to insert data from " + config.rawDataFile;
            return result;
        }

        if (groundTruth == null) {
            result.failedReason = "GroundTruth is null";
            return result;
        }
        if (queryVectors == null) {
            result.failedReason = "QueryVectors is null";
            return result;
        }

        recallAndLatencyTest();
        qpsTest();
        return result;
    }
}

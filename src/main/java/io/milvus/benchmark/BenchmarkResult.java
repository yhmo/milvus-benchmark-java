package io.milvus.benchmark;

public class BenchmarkResult {
    public float averageLatency = 0.0f;
    public float averageRecall = 0.0f;

    public float qps = 0;
    public long totalExecutedRequests = 0L;

    public String failedReason;
}

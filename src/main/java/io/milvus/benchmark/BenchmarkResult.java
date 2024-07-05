package io.milvus.benchmark;

public class BenchmarkResult {
    public int qps = 0;
    public long totalRequests = 0L;

    public float perLatency = 0.0f;
    public float recall = 0.0f;

    public String failedReason;
}

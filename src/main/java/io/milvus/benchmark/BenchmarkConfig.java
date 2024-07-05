package io.milvus.benchmark;

public class BenchmarkConfig {
    // general configurations
    public String collectionName;
    public String rawDataFile;
    public String queryVectorFile;
    public String groundTruthFile;
    public int nq = 1;
    public int topK = 100;

    // recall/latency test configurations
    public int latencyRepeat = 100;

    // qps test configurations
    public int qpsThreadsCount = 10;
    public int qpsTestSeconds = 30;
}

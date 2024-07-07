package io.milvus.benchmark;

import java.util.ArrayList;
import java.util.List;

public class BenchmarkConfig {
    // general configurations
    public String collectionName;
    public List<String> rawDataFiles = new ArrayList<>();
    public String queryVectorFile;
    public String groundTruthFile;
    public int nq = 1;
    public int topK = 100;

    // recall/latency test configurations
    public int latencyRepeat = 1000;

    // qps test configurations
    public int qpsThreadsCount = 10;
    public int qpsTestSeconds = 30;
}

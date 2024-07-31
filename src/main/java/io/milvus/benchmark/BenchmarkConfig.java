package io.milvus.benchmark;

import io.milvus.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BenchmarkConfig {
    // dataset configurations
    public String collectionName;
    public List<String> rawDataFiles = new ArrayList<>();
    public String queryVectorFile;
    public String groundTruthFile;
    public boolean dropCollectionIfExists = true;
    public boolean skipImport = true;
    public int batchSize = 50; // units: MB

    // recall/latency test configurations
    public int latencyTopK = 100;

    // qps test configurations
    public int qpsThreadsCount = 10;
    public int qpsTestSeconds = 30;
    public int qpsNq = 1;
    public int qpsTopK = 100;

    public BenchmarkConfig() {
        init();
        download();
    }

    private void init() {
        Map<String, Object> configurations = Utils.readConfigurations();

        Map<String, Object> dataSet = (Map<String, Object>)configurations.get("dataSet");
        collectionName = (String)dataSet.get("collectionName");
        rawDataFiles = (List<String>)dataSet.get("originalDataURL");
        queryVectorFile = (String)dataSet.get("queryVectorsURL");
        groundTruthFile = (String)dataSet.get("groundTruthURL");
        dropCollectionIfExists = (Boolean)dataSet.get("dropCollectionIfExists");
        skipImport = (Boolean)dataSet.get("skipImport");
        batchSize = (Integer) dataSet.get("batchSize");

        Map<String, Object> latencyTest = (Map<String, Object>)configurations.get("latencyTest");
        latencyTopK = (Integer) latencyTest.get("topK");

        Map<String, Object> qpsTest = (Map<String, Object>)configurations.get("qpsTest");
        qpsThreadsCount = (Integer) qpsTest.get("threads");
        qpsTopK = (Integer) qpsTest.get("topK");
        qpsNq = (Integer) qpsTest.get("nq");

//        System.out.println(configurations);
    }

    private void download() {
        String localDir = "data/" + collectionName;
        if (!skipImport) {
            List<String> localFiles = new ArrayList<>();
            for (String url : rawDataFiles) {
                String localPath = Utils.downloadRemoteFile(url, localDir);
                if (!localPath.isEmpty()) {
                    localFiles.add(localPath);
                }
            }
            rawDataFiles = localFiles;
        }

        queryVectorFile = Utils.downloadRemoteFile(queryVectorFile, localDir);
        groundTruthFile = Utils.downloadRemoteFile(groundTruthFile, localDir);
    }
}

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
    public boolean skipImport = true;

    // recall/latency test configurations
    public int latencyRepeat = 1000;
    public int latencyNq = 1;
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
        skipImport = (Boolean)dataSet.get("skipImport");

        Map<String, Object> latencyTest = (Map<String, Object>)configurations.get("latencyTest");
        latencyRepeat = (Integer) latencyTest.get("repeat");
        latencyTopK = (Integer) latencyTest.get("topK");
        latencyNq = (Integer) latencyTest.get("nq");

        Map<String, Object> qpsTest = (Map<String, Object>)configurations.get("qpsTest");
        qpsThreadsCount = (Integer) qpsTest.get("threads");
        qpsTopK = (Integer) qpsTest.get("topK");
        qpsNq = (Integer) qpsTest.get("nq");

//        System.out.println(configurations);
    }

    private void download() {
        String localDir = "data/" + collectionName;
        List<String> localFiles = new ArrayList<>();
        for (String url : rawDataFiles) {
            String localPath = Utils.downloadRemoteFile(url, localDir);
            if (!localPath.isEmpty()) {
                localFiles.add(localPath);
            }
        }
        rawDataFiles = localFiles;

        String localQueryVectorsPath = Utils.downloadRemoteFile(queryVectorFile, localDir);
        queryVectorFile = localQueryVectorsPath;

        String localGroundTruthPath = Utils.downloadRemoteFile(groundTruthFile, localDir);
        groundTruthFile = localGroundTruthPath;
    }
}

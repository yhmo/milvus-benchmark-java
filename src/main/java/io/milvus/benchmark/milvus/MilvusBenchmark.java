package io.milvus.benchmark.milvus;

import io.milvus.Utils;
import io.milvus.benchmark.*;
import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.SearchResults;
import io.milvus.ingestion.milvus.MilvusIngestion;
import io.milvus.param.ConnectParam;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.collection.LoadCollectionParam;
import io.milvus.param.dml.SearchParam;
import io.milvus.parser.ParquetParser;
import io.milvus.parser.Parser;
import io.milvus.response.DescCollResponseWrapper;
import io.milvus.response.SearchResultsWrapper;

import java.util.*;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;

public class MilvusBenchmark extends Benchmark {
    private final ConnectParam connect;

    public MilvusBenchmark() {
        super(new BenchmarkConfig());

        MilvusConfig milvusConfig = new MilvusConfig();
        this.connect = milvusConfig.connectParam;

        List<Parser> parsers = new ArrayList<>();
        for (String filePath : config.rawDataFiles) {
            parsers.add(new ParquetParser(filePath));
        }

        ingestion = new MilvusIngestion(connect, config, parsers);
        groundTruth = new GroundTruth(new ParquetParser(config.groundTruthFile));
        queryVectors = new QueryVectors(new ParquetParser(config.queryVectorFile));
    }

    @Override
    protected void preTest() {
        MilvusClient milvusClient = new MilvusServiceClient(connect);
        milvusClient.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(config.collectionName)
                .withSyncLoadWaitingTimeout(300L)
                .build());
        System.out.println("Collection loaded");
        milvusClient.close();
    }

    @Override
    protected void recallAndLatencyTest() {
        MilvusClient milvusClient = new MilvusServiceClient(connect);

        System.out.println(String.format("Prepare to search %d times to test average latency and recall",
                config.latencyRepeat));

        R<DescribeCollectionResponse> descCol = milvusClient.describeCollection(DescribeCollectionParam.newBuilder()
                .withCollectionName(config.collectionName)
                .build());
        if (descCol.getStatus() != R.Status.Success.getCode()) {
            System.out.println(String.format("Failed to get collection '%s', error: %s",
                    config.collectionName, descCol.getMessage()));
            return;
        }
        DescCollResponseWrapper wrapper = new DescCollResponseWrapper(descCol.getData());
        List<FieldType> vectorFields = wrapper.getVectorFields();
        String vectorFieldName = vectorFields.get(0).getName();

        Map<Long, List<Float>> queryVectors = this.queryVectors.getVectors();
        List<Long> queryIDs = this.queryVectors.getIDs();
        Map<Long, List<Long>> truth = groundTruth.getTruth();

        Random ran = new Random();
        float totalRecall = 0.0f;
        float totalLatency = 0.0f;
        for (int i = 0; i < config.latencyRepeat; i++) {
            List<List<Float>> targetVectors = new ArrayList<>();
            List<Long> targetIDs = new ArrayList<>();
            for (int k = 0; k < config.latencyNq; k++) {
                Long id = queryIDs.get(ran.nextInt(queryIDs.size()));
                targetIDs.add(id);
                targetVectors.add(queryVectors.get(id));
            }

            SearchParam searchParam = SearchParam.newBuilder()
                    .withCollectionName(config.collectionName)
                    .withMetricType(MetricType.COSINE)
                    .withTopK(config.latencyTopK)
                    .withFloatVectors(targetVectors)
                    .withVectorFieldName(vectorFieldName)
                    .withParams("{}")
                    .build();
            long tsStart = System.nanoTime();
            R<SearchResults> searchResp = milvusClient.search(searchParam);
            if (searchResp.getStatus() != R.Status.Success.getCode()) {
                System.out.println("Failed to search: " + searchResp.getMessage());
            }
            long tsEnd = System.nanoTime();
            totalLatency += (tsEnd - tsStart)/1000000; // convert nano second to milli second

            int correct = 0;
            SearchResultsWrapper searchWrapper = new SearchResultsWrapper(searchResp.getData().getResults());
            for (int k = 0; k < config.latencyNq; ++k) {
                List<SearchResultsWrapper.IDScore> scores = searchWrapper.getIDScore(k);
                Long targetID = targetIDs.get(k);
                List<Long> allTruth = truth.get(targetID);
                List<Long> topkTruth = allTruth.subList(0, config.latencyTopK);
                for (SearchResultsWrapper.IDScore score : scores) {
                    long resultID = score.getLongID();
                    if (topkTruth.contains(resultID)) {
                        correct++;
                    }
                }
            }
            float recallRate = (float)correct/ (config.latencyNq* config.latencyTopK);
            totalRecall += recallRate;
        }
        float averageRecall = totalRecall/config.latencyRepeat;
        float averageLatency = totalLatency/ config.latencyRepeat;
        System.out.println(String.format("Repeat %d times, average recall rate: %.2f%%, average latency: %.1f ms",
                config.latencyRepeat, averageRecall*100.0f, averageLatency));

        this.result.averageRecall = averageRecall;
        this.result.averageLatency = averageLatency;

        milvusClient.close();
    }

    @Override
    protected void qpsTest() {
        System.out.println(String.format("Prepare to test qps with %d concurrent threads, %d seconds per thread",
                config.qpsThreadsCount, config.qpsTestSeconds));
        List<SearchRunner> runners = new ArrayList<>();
        for (int i = 0; i < config.qpsThreadsCount; i++) {
            SearchRunner runner = new MilvusSearchRunner(connect, queryVectors, config);
            runners.add(runner);
        }

        long tsStart = System.currentTimeMillis();
        List<Thread> threads = new ArrayList<>();
        for (SearchRunner runner : runners) {
             Thread t = new Thread(runner);
             threads.add(t);
             t.start();
        }

        try {
            for (Thread t : threads) {
                t.join();
            }
        } catch (InterruptedException e) {
            System.out.println("Thread interrupted");
            e.printStackTrace();
        }
        long tsEnd = System.currentTimeMillis();

        long totalExecutedRequests = 0L;
        for (SearchRunner runner : runners) {
            totalExecutedRequests += runner.executedReuests();
            if (runner.failedReason() != null) {
                result.failedReason = runner.failedReason();
            }
        }
        result.totalExecutedRequests = totalExecutedRequests;
        float elapsedSeconds = (float)(tsEnd - tsStart)/1000;
        result.qps = (float)totalExecutedRequests/ elapsedSeconds;

        System.out.println(String.format("%d threads, %.3f seconds elapsed, total executed: %d, average qps: %.3f",
                config.qpsThreadsCount, elapsedSeconds, totalExecutedRequests, result.qps));
    }

    @Override
    protected void postRun() {
        // print report
        if (result.failedReason != null) {
            System.out.println("Benchmark failed. " + result.failedReason);
        } else {
            System.out.println("#########################################################################################");
            System.out.println("Latency test:");
            System.out.println(String.format("\tRepeat %d times, nq=%d, topK=%d",
                    config.latencyRepeat, config.latencyNq, config.latencyTopK));
            System.out.println(String.format("\tAverage recall rate: %.2f%%", result.averageRecall*100.0f));
            System.out.println(String.format("\tAverage latency: %.1f ms", result.averageLatency));
            System.out.println("#########################################################################################");
            System.out.println("QPS test:");
            System.out.println(String.format("\t%d threads, %d seconds, nq=%d, topK=%d",
                    config.qpsThreadsCount, config.qpsTestSeconds, config.qpsNq, config.qpsTopK));
            System.out.println(String.format("\tQPS: %.3f", result.qps));
            System.out.println("#########################################################################################");
        }

        // report to md file
        String baseDir = Utils.generatorLocalPath("report");
        String filePath = String.format("%s/%s.md", baseDir, config.collectionName);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            if (result.failedReason != null) {
                writer.write("Benchmark failed. " + result.failedReason);
            } else {
                writer.write("## Latency test:\n");
                writer.write(String.format("  Repeat %d times, nq=%d, topK=%d\n",
                        config.latencyRepeat, config.latencyNq, config.latencyTopK));
                writer.write(String.format("  Average recall rate: %.2f%%\n", result.averageRecall*100.0f));
                writer.write(String.format("  Average latency: %.1f ms\n", result.averageLatency));
                writer.write("## QPS test:\n");
                writer.write(String.format("  %d threads, %d seconds, nq=%d, topK=%d\n",
                        config.qpsThreadsCount, config.qpsTestSeconds, config.qpsNq, config.qpsTopK));
                writer.write(String.format("  QPS: %.3f\n", result.qps));
            }
            
            Date currentDate = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            writer.write(String.format("\nTest date: \n", sdf.format(currentDate)));
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println(String.format("Failed to export report file, error: %s", filePath, e.getMessage()));
        }
    }
}

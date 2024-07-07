package io.milvus.benchmark.milvus;

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
import io.milvus.param.dml.SearchParam;
import io.milvus.parser.ParquetParser;
import io.milvus.parser.Parser;
import io.milvus.response.DescCollResponseWrapper;
import io.milvus.response.SearchResultsWrapper;

import java.util.*;

public class MilvusBenchmark extends Benchmark {
    private final ConnectParam connect;

    public MilvusBenchmark(ConnectParam connect, BenchmarkConfig config) {
        super(config);
        this.connect = connect;

        List<Parser> parsers = new ArrayList<>();
        for (String filePath : config.rawDataFiles) {
            parsers.add(new ParquetParser(filePath));
        }

        ingestion = new MilvusIngestion(connect, config.collectionName, parsers);
        groundTruth = new GroundTruth(new ParquetParser(config.groundTruthFile));
        queryVectors = new QueryVectors(new ParquetParser(config.queryVectorFile));
    }

    @Override
    protected void recallAndLatencyTest() {
        MilvusClient milvusClient = new MilvusServiceClient(connect);

        System.out.println(String.format("Prepare to search %d times to test average latency and recall",
                config.latencyRepeat));

        R<DescribeCollectionResponse> descCol = milvusClient.describeCollection(DescribeCollectionParam.newBuilder()
                .withCollectionName(config.collectionName)
                .build());
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
            totalLatency = (tsEnd - tsStart)/1000000;

            int correct = 0;
            SearchResultsWrapper searchWrapper = new SearchResultsWrapper(searchResp.getData().getResults());
            for (int k = 0; k < config.latencyNq; ++k) {
                List<SearchResultsWrapper.IDScore> scores = searchWrapper.getIDScore(k);
                Long targetID = targetIDs.get(k);
                List<Long> nTruth = truth.get(targetID);
                for (SearchResultsWrapper.IDScore score : scores) {
                    long resultID = score.getLongID();
                    if (nTruth.contains(resultID)) {
                        correct++;
                    }
                }
            }
            float recallRate = (float)correct/ (config.latencyNq* config.latencyTopK);
            totalRecall += recallRate;
        }
        float averageRecall = totalRecall/config.latencyRepeat;
        float averageLatency = totalLatency/ config.latencyRepeat;
        System.out.println(String.format("Repeat %d times, average recall rate: %f, average latency: %f ms",
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

        long totalExecutedRequests = 0L;
        for (SearchRunner runner : runners) {
            totalExecutedRequests += runner.executedReuests();
        }
        result.totalExecutedRequests = totalExecutedRequests;
        result.qps = (float)totalExecutedRequests/ config.qpsTestSeconds;

        System.out.println(String.format("%d threads run %d seconds, total executed: %d, average qps: %f",
                config.qpsThreadsCount, config.qpsTestSeconds, totalExecutedRequests, result.qps));
    }
}

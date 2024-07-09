package io.milvus.benchmark.milvus;

import io.milvus.benchmark.BenchmarkConfig;
import io.milvus.benchmark.QueryVectors;
import io.milvus.benchmark.SearchRunner;
import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.SearchResults;
import io.milvus.param.ConnectParam;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.dml.SearchParam;
import io.milvus.response.DescCollResponseWrapper;

import java.util.*;

public class MilvusSearchRunner extends SearchRunner {
    private MilvusClient milvusClient;
    private long executedRequests = 0L;
    private String vectorFieldName;
    private List<Float> targetVector;

    public MilvusSearchRunner(ConnectParam connect, QueryVectors queryVectors, BenchmarkConfig config) {
        super(queryVectors, config);
        milvusClient = new MilvusServiceClient(connect);

        prepare();
    }

    private void prepare() {
        R<DescribeCollectionResponse> descCol = milvusClient.describeCollection(DescribeCollectionParam.newBuilder()
                .withCollectionName(config.collectionName)
                .build());
        if (descCol.getStatus() != R.Status.Success.getCode()) {
            failedReason = String.format("Failed to get collection '%s', error: %s",
                    config.collectionName, descCol.getMessage());
            System.out.println(failedReason);
            return;
        }

        DescCollResponseWrapper wrapper = new DescCollResponseWrapper(descCol.getData());
        List<FieldType> vectorFields = wrapper.getVectorFields();
        vectorFieldName = vectorFields.get(0).getName();

        Map<Long, List<Float>> qVectors = this.queryVectors.getVectors();
        List<Long> queryIDs = this.queryVectors.getIDs();
        Long targetID = queryIDs.get(new Random().nextInt(queryIDs.size()));
        targetVector = qVectors.get(targetID);
    }

    public void run() {
        if (vectorFieldName == null || targetVector == null) {
            return;
        }

        System.out.println(String.format("Thread '%s' start", Thread.currentThread().getName()));
        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(config.collectionName)
                .withMetricType(MetricType.COSINE)
                .withTopK(config.qpsTopK)
                .withFloatVectors(Collections.singletonList(targetVector))
                .withVectorFieldName(vectorFieldName)
                .withParams("{}")
                .build();

        long tsStart = System.currentTimeMillis();
        while (true) {
            long elapsed = System.currentTimeMillis() - tsStart;
            if (elapsed >= config.qpsTestSeconds* 1000L) {
                System.out.println(String.format("Thread '%s' finished in %f seconds",
                        Thread.currentThread().getName(), (float)elapsed/1000));
                break;
            }

            R<SearchResults> searchResp = milvusClient.search(searchParam);
            if (searchResp.getStatus() != R.Status.Success.getCode()) {
                failedReason = "Failed to search: " + searchResp.getMessage();
                System.out.println(failedReason);
                break;
            }
            this.executedRequests++;
        }

        System.out.println(String.format("Thread '%s' stop, %d requests executed, %f seconds elapsed",
                Thread.currentThread().getName(), executedRequests, (float)(System.currentTimeMillis() - tsStart)/1000));
    }

    @Override
    public long executedReuests() {
        return this.executedRequests;
    }
}

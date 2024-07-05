package io.milvus.benchmark;

import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.SearchResults;
import io.milvus.ingestion.MilvusIngestion;
import io.milvus.param.ConnectParam;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.collection.DescribeCollectionParam;
import io.milvus.param.collection.FieldType;
import io.milvus.param.dml.SearchParam;
import io.milvus.parser.ParquetParser;
import io.milvus.response.DescCollResponseWrapper;
import io.milvus.response.SearchResultsWrapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class MilvusBenchmark extends Benchmark {
    private final ConnectParam connect;

    public MilvusBenchmark(ConnectParam connect, BenchmarkConfig config) {
        super(config);
        this.connect = connect;

        ingestion = new MilvusIngestion(connect, config.collectionName, new ParquetParser(config.rawDataFile));
        groundTruth = new GroundTruth(new ParquetParser(config.groundTruthFile));
        queryVectors = new QueryVectors(new ParquetParser(config.queryVectorFile));
    }

    @Override
    protected void recallAndLatencyTest() {
        MilvusClient milvusClient = new MilvusServiceClient(connect);

        R<DescribeCollectionResponse> descCol = milvusClient.describeCollection(DescribeCollectionParam.newBuilder()
                .withCollectionName(config.collectionName)
                .build());
        DescCollResponseWrapper wrapper = new DescCollResponseWrapper(descCol.getData());
        List<FieldType> vectorFields = wrapper.getVectorFields();
        String vectorFieldName = vectorFields.get(0).getName();

        List<List<Float>> queryVectors = this.queryVectors.getVectors();
        Random ran = new Random();
        List<List<Float>> targetVectors = new ArrayList<>();
        for (int i = 0; i < config.nq; i++) {
            targetVectors.add(queryVectors.get(ran.nextInt()));
        }

        SearchParam searchParam = SearchParam.newBuilder()
                .withCollectionName(config.collectionName)
                .withMetricType(MetricType.COSINE)
                .withTopK(config.topK)
                .withFloatVectors(targetVectors)
                .withVectorFieldName(vectorFieldName)
                .withParams("{}")
                .build();


        for (int i = 0; i < config.latencyRepeat; i++) {
            R<SearchResults> searchResp = milvusClient.search(searchParam);

            SearchResultsWrapper searchWrapper = new SearchResultsWrapper(searchResp.getData().getResults());
            for (int k = 0; k < config.nq; ++k) {
                List<SearchResultsWrapper.IDScore> scores = searchWrapper.getIDScore(i);
            }
        }

        milvusClient.close();
    }

    @Override
    protected void qpsTest() {

    }
}

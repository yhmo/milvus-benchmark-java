package io.milvus;

import io.milvus.benchmark.BenchmarkConfig;
import io.milvus.benchmark.MilvusBenchmark;
import io.milvus.param.ConnectParam;

public class Main {

    public static void main(String[] args) {
        System.out.println("Vector database benchmark program start...");

        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(19530)
                .build();

        BenchmarkConfig config = new BenchmarkConfig();
        config.collectionName = "milvus_benchmark_java";
        String baseDir = Utils.generatorLocalPath("data");
        config.rawDataFile = baseDir + "/shuffle_train.parquet";
        config.groundTruthFile = baseDir + "/neighbors.parquet";
        config.queryVectorFile = baseDir + "/test.parquet";

        MilvusBenchmark benchmark = new MilvusBenchmark(connectParam, config);
        benchmark.run();

        System.out.println("Vector database benchmark program completed.");
    }
}
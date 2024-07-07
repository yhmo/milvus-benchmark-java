package io.milvus;

import io.milvus.benchmark.BenchmarkConfig;
import io.milvus.benchmark.milvus.MilvusBenchmark;
import io.milvus.param.ConnectParam;
import io.milvus.parser.ParquetParser;
import io.milvus.parser.Parser;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public class Main {

    public static void main(String[] args) {
        System.out.println("Vector database benchmark program start...");

        ConnectParam connectParam = ConnectParam.newBuilder()
                .withHost("localhost")
                .withPort(19530)
                .build();

        BenchmarkConfig config = new BenchmarkConfig();
        MilvusBenchmark benchmark = new MilvusBenchmark(connectParam, config);
        benchmark.run();

        System.out.println("Vector database benchmark program completed.");
    }
}
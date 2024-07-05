package io.milvus.benchmark;

import io.milvus.parser.ParquetParser;
import io.milvus.parser.Parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryVectors {
    private List<List<Float>> vectors;

    public QueryVectors(Parser parser) {
        parseQueryVectors(parser);
    }

    public List<List<Float>> getVectors() {
        return vectors;
    }

    private void parseQueryVectors(Parser parser) {
        vectors = new ArrayList<>();

        System.out.println("Prepare query vectors...");
        Map<String, Object> row = parser.nextRow();
        while(row != null) {
//            Long id = (Long)row.get("id");
            List<Float> vector = (List<Float>)row.get("emb");
            vectors.add(vector);
            row = parser.nextRow();
        }
        System.out.println(String.format("Query vectors count: %d", vectors.size()));
    }
}

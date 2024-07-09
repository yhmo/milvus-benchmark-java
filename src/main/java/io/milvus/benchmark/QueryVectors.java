package io.milvus.benchmark;

import io.milvus.Constants;
import io.milvus.parser.ParquetParser;
import io.milvus.parser.Parser;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryVectors {
    private Map<Long, List<Float>> vectors = new HashMap<>();

    public QueryVectors(Parser parser) {
        parseQueryVectors(parser);
    }

    public Map<Long, List<Float>> getVectors() {
        return vectors;
    }
    public List<Long> getIDs() {
        return new ArrayList<>(vectors.keySet());
    }

    private void parseQueryVectors(Parser parser) {
        System.out.println("Prepare query vectors...");
        Map<String, Object> row = parser.nextRow();
        while(row != null) {
            Long id = (Long)row.get(Constants.ID_FIELD);
            List<Float> vector = (List<Float>)row.get("emb");
            vectors.put(id, vector);
            row = parser.nextRow();
        }
        System.out.println(String.format("Query vectors count: %d", vectors.size()));
    }
}

package io.milvus.benchmark;

import io.milvus.Constants;
import io.milvus.parser.ParquetParser;
import io.milvus.parser.Parser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GroundTruth {
    private Map<Long, List<Long>> truth;

    public GroundTruth(Parser parser) {
        parseGroundTruth(parser);
    }

    public Map<Long, List<Long>> getTruth() {
        return truth;
    }

    private void parseGroundTruth(Parser parser) {
        truth = new HashMap<>();

        System.out.println("Prepare ground truth...");
        Map<String, Object> row = parser.nextRow();
        while(row != null) {
            Long id = (Long)row.get(Constants.ID_FIELD);
            List<Long> neighbors = (List<Long>)row.get(Constants.NEIGHBORS_ID);
            truth.put(id, neighbors);
            row = parser.nextRow();
        }
        System.out.println(String.format("Ground truth count: %d", truth.size()));
    }
}

package io.milvus.ingestion;

import io.milvus.parser.Parser;
import io.milvus.parser.RawFieldType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class Ingestion {
    protected final Parser parser;

    public Ingestion(Parser parser) {
        this.parser = parser;
    }

    protected void preRun() {}
    protected abstract boolean createCollection(Map<String, RawFieldType> rawSchema);
    protected abstract boolean createIndex();
    protected abstract boolean insertRows(List<Map<String, Object>> rows);

    protected void postRun() {}

    private int estimateRowSize(Map<String, RawFieldType> rawSchema) {
        int size = 0;
        for (String key : rawSchema.keySet()) {
            RawFieldType field = rawSchema.get(key);
            String primitiveName = field.primitiveTypeName;
            String logicName = field.logicTypeName;
            if (logicName.equals("LIST")) {
                if (primitiveName.equals("FLOAT") || primitiveName.equals("DOUBLE")) {
                    size += 4 * field.dimension;
                }
            } else {
                if (primitiveName.equals("BINARY") && logicName.equals("STRING")) {
                    size += 32; // hardcode
                }
                if (primitiveName.equals("BOOLEAN")) {
                    size += 1;
                } else if (primitiveName.equals("INT32") || primitiveName.equals("FLOAT")) {
                    size += 4;
                } else if (primitiveName.equals("INT64") || primitiveName.equals("DOUBLE")) {
                    size += 8;
                }
            }
        }
        return size;
    }

    public boolean run() {
        preRun(); // a chance to do something before run

        Map<String, Object> row =  parser.nextRow(); // must call before rowSchema()
        Map<String, RawFieldType> rawSchema = parser.rawSchema();
        if (!createCollection(rawSchema)) {
            System.out.println("Failed to create collection");
            return false;
        }

        int rowSize = estimateRowSize(rawSchema);
        int batchRows = 50 * 1024 * 1024 / rowSize;
        System.out.println(String.format("Estimated row size: %d, rows per batch: %d", rowSize, batchRows));

        List<Map<String, Object>> rows = new ArrayList<>();
        int rowCounter = 0;
        while (row != null) {
            rows.add(row);

            if (rowCounter > 10000) {
                break;
            }

            if (rows.size() >= batchRows) {
                if(!insertRows(rows)) {
                    System.out.println("Failed to insert");
                    return false;
                }
                rowCounter += rows.size();
                rows.clear();
                System.out.println(String.format("%d rows inserted", rowCounter));
            }
            row = parser.nextRow();
        }

        // the tail data
        if (!rows.isEmpty()) {
            if(!insertRows(rows)) {
                System.out.println("Failed to insert");
                return false;
            }
            rowCounter += rows.size();
            rows.clear();
        }
        System.out.println(String.format("Totally %d rows inserted", rowCounter));

        if (!createIndex()) {
            System.out.println("Failed to create index");
            return false;
        }

        postRun(); // a chance to do something after run
        return true;
    }
}

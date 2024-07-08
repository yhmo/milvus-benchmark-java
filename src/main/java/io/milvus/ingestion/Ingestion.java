package io.milvus.ingestion;

import io.milvus.parser.Parser;
import io.milvus.parser.RawFieldType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class Ingestion {
    protected final List<Parser> parsers;
    protected int batchSize = 50; // units: MB

    public Ingestion(List<Parser> parsers, int batchSize) {
        this.parsers = parsers;
        this.batchSize = batchSize;
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
        if (parsers.isEmpty()) {
            System.out.println("Parser list is empty");
            return false;
        }
        System.out.println("Prepare to run ingestion...");
        preRun(); // a chance to do something before run

        List<Map<String, Object>> rows = new ArrayList<>();
        Parser firstParser = parsers.get(0);
        Map<String, Object> row =  firstParser.nextRow(); // must call before rowSchema()
        rows.add(row); // don't forget this row
        Map<String, RawFieldType> rawSchema = firstParser.rawSchema();
        if (!createCollection(rawSchema)) {
            System.out.println("Failed to create collection");
            return false;
        }

        int rowSize = estimateRowSize(rawSchema);
        int batchRows = batchSize * 1024 * 1024 / rowSize;
        System.out.println(String.format("Estimated row size: %d, rows per batch: %d", rowSize, batchRows));

        int rowCounter = 0;
        for (Parser parser : parsers) {
            row =  parser.nextRow();
            while (row != null) {
                rows.add(row);

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
                System.out.println(String.format("%d rows inserted", rowCounter));
            }
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

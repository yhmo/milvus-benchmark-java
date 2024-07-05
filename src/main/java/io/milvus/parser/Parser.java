package io.milvus.parser;

import java.util.Map;

public abstract class Parser {
    private String filePath;

    public Parser(String filePath) {
        this.filePath = filePath;
    }

    public String getFilePath() {
        return this.filePath;
    }

    public abstract Map<String, Object> nextRow();
    public abstract Map<String, RawFieldType> rawSchema();
}

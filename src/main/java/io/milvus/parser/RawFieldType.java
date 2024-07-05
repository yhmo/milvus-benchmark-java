package io.milvus.parser;

public class RawFieldType {
    public String primitiveTypeName = "";
    public String logicTypeName = "";
    public int dimension = 0;

    public RawFieldType(String primitiveType, String logicType) {
        primitiveTypeName = primitiveType;
        logicTypeName = logicType;
    }

    @Override
    public String toString() {
        return String.format("{RawFieldType: %s, LogicTypeName: %s, Dimension: %d}",
                primitiveTypeName, logicTypeName, dimension);
    }
}

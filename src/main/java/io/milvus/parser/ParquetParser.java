package io.milvus.parser;

import com.google.gson.Gson;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetReader.Builder;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParquetParser extends Parser {

    private ParquetReader<Group> parquetReader;

    private Gson GSON_INSTANCE = new Gson();
    private int rowCounter = 0;

    private Map<String, RawFieldType> parquetFieldTypes = new HashMap<>();

    public Map<String, RawFieldType> rawSchema() {
        return parquetFieldTypes;
    }

    public ParquetParser(String filePath) {
        super(filePath);
        try {
            GroupReadSupport support = new GroupReadSupport();
            Builder<Group> builder = ParquetReader.builder(support, new Path(filePath));
            parquetReader = builder.build();
        } catch (Exception e) {
            System.out.println(String.format("ParquetParser initializer error: %s", e.getMessage()));
        }
    }

    public Map<String, Object> nextRow() {
        try {
            Group group = parquetReader.read();
            if (group == null) {
                System.out.println(String.format("Totally %d rows read from '%s'", rowCounter, getFilePath()));
                return null;
            }
            rowCounter++;

            Map<String, Object> row = new HashMap<>();
            GroupType groupType = group.getType();
            for (int i = 0; i < groupType.getFieldCount(); i++) {
                String fieldName = groupType.getFieldName(i);
                Type tt = groupType.getType(fieldName);
                LogicalTypeAnnotation logic = tt.getLogicalTypeAnnotation();
                String logicName = (logic ==null) ? "" : logic.toString();
                String primitiveName = "";
                if (tt.isPrimitive()) {

                } else {
                    if (logicName.equals("LIST")) {
                        while(!tt.isPrimitive()) {
                            tt = tt.asGroupType().getType(0);
                        }
                    }
                }
                primitiveName = tt.asPrimitiveType().getPrimitiveTypeName().name();
                parquetFieldTypes.put(fieldName, new RawFieldType(primitiveName, logicName));

                if (logicName.equals("LIST")) {
                    if (primitiveName.equals("FLOAT") || primitiveName.equals("DOUBLE")) {
                        Group subGroup = group.getGroup(fieldName, 0);
                        List<Float> vector = readFloatVector(fieldName, subGroup, primitiveName.equals("FLOAT"));
                        row.put(fieldName, vector);
                    }
                    if (primitiveName.equals("INT64")) {
                        Group subGroup = group.getGroup(fieldName, 0);
                        List<Long> array = readInt64Array(fieldName, subGroup);
                        row.put(fieldName, array);
                    }
                } else {
                    if (primitiveName.equals("BINARY") && logicName.equals("STRING")) {
                        String val = group.getString(fieldName, 0);
                        row.put(fieldName, val);
                    }

                    if (primitiveName.equals("BOOLEAN")) {
                        Boolean val = group.getBoolean(fieldName, 0);
                        row.put(fieldName, val);
                    } else if (primitiveName.equals("INT32")) {
                        Integer val = group.getInteger(fieldName, 0);
                        row.put(fieldName, val);
                    } else if (primitiveName.equals("INT64")) {
                        Long val = group.getLong(fieldName, 0);
                        row.put(fieldName, val);
                    } else if (primitiveName.equals("FLOAT")) {
                        Float val = group.getFloat(fieldName, 0);
                        row.put(fieldName, val);
                    } else if (primitiveName.equals("DOUBLE")) {
                        Double val = group.getDouble(fieldName, 0);
                        row.put(fieldName, val);
                    }
                }
            }
            return row;
        } catch (Exception e) {
            System.out.println(String.format("ParquetParser read error: %s", e.getMessage()));
            return null;
        }
    }

    private List<Long> readInt64Array(String fieldName, Group group) {
        int dimension = parquetFieldTypes.get(fieldName).dimension;
        List<Long> array = new ArrayList<>();
        if (dimension == 0) {
            for (int d = 0; d < 4096; d++) {
                try {
                    Group element = group.getGroup(0, d);
                    Long val = element.getLong(0, 0);
                    array.add(val);
                } catch (RuntimeException ignored) {
                    parquetFieldTypes.get(fieldName).dimension = d;
                    break;
                }
            }
        } else {
            for (int d = 0; d < dimension; d++) {
                Group element = group.getGroup(0, d);
                Long val = element.getLong(0, 0);
                array.add(val);
            }
        }

        return array;
    }

    private List<Float> readFloatVector(String fieldName, Group group, boolean isFloat) {
        int dimension = parquetFieldTypes.get(fieldName).dimension;
        List<Float> vector = new ArrayList<>();
        if (dimension == 0) {
            for (int d = 0; d < 65535; d++) {
                try {
                    Group element = group.getGroup(0, d);
                    Float val = isFloat ? element.getFloat(0, 0) : (float) element.getDouble(0, 0);
                    vector.add(val);
                } catch (RuntimeException ignored) {
                    parquetFieldTypes.get(fieldName).dimension = d;
                    break;
                }
            }
        } else {
            for (int d = 0; d < dimension; d++) {
                Group element = group.getGroup(0, d);
                Float val = isFloat ? element.getFloat(0, 0) : (float) element.getDouble(0, 0);
                vector.add(val);
            }
        }

        return vector;
    }
}

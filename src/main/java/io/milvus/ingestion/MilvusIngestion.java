package io.milvus.ingestion;

import com.alibaba.fastjson.JSONObject;
import io.milvus.client.MilvusClient;
import io.milvus.client.MilvusServiceClient;
import io.milvus.common.clientenum.ConsistencyLevelEnum;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.index.CreateIndexParam;
import io.milvus.param.index.GetIndexStateParam;
import io.milvus.parser.Parser;
import io.milvus.parser.RawFieldType;
import io.milvus.response.GetCollStatResponseWrapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MilvusIngestion extends Ingestion {
    private MilvusClient milvusClient;
    private CreateCollectionParam param;
    private String collectionName;

    public MilvusIngestion(ConnectParam connect, String collectionName, Parser parser) {
        super(parser);
        milvusClient = new MilvusServiceClient(connect);
        this.collectionName = collectionName;
    }

    private CollectionSchemaParam convertSchema(Map<String, RawFieldType> rawSchema) {
        boolean enabledDynamic = false;
        CollectionSchemaParam.Builder builder = CollectionSchemaParam.newBuilder();
        for (String key : rawSchema.keySet()) {
            if (key.equals(Constant.DYNAMIC_FIELD_NAME)) {
                enabledDynamic = true;
                continue;
            }

            boolean primaryKey = key.equals("id");

            RawFieldType field = rawSchema.get(key);
            String primitiveName = field.primitiveTypeName;
            String logicName = field.logicTypeName;
            if (logicName.equals("LIST")) {
                if (primitiveName.equals("FLOAT") || primitiveName.equals("DOUBLE")) {
                    builder.addFieldType(FieldType.newBuilder()
                            .withName(key)
                            .withDataType(DataType.FloatVector)
                            .withDimension(field.dimension)
                            .build());
                }
            } else {
                if (primitiveName.equals("BINARY") && logicName.equals("STRING")) {
                    builder.addFieldType(FieldType.newBuilder()
                            .withName(key)
                            .withDataType(DataType.VarChar)
                            .withPrimaryKey(primaryKey)
                            .withMaxLength(65535)
                            .build());
                }
                switch (primitiveName) {
                    case "BOOLEAN":
                        builder.addFieldType(FieldType.newBuilder()
                                .withName(key)
                                .withDataType(DataType.Bool)
                                .build());
                        break;
                    case "INT32":
                        builder.addFieldType(FieldType.newBuilder()
                                .withName(key)
                                .withDataType(DataType.Int32)
                                .build());
                        break;
                    case "FLOAT":
                        builder.addFieldType(FieldType.newBuilder()
                                .withName(key)
                                .withDataType(DataType.Float)
                                .build());
                        break;
                    case "INT64":
                        builder.addFieldType(FieldType.newBuilder()
                                .withName(key)
                                .withDataType(DataType.Int64)
                                .withPrimaryKey(primaryKey)
                                .build());
                        break;
                    case "DOUBLE":
                        builder.addFieldType(FieldType.newBuilder()
                                .withName(key)
                                .withDataType(DataType.Double)
                                .build());
                        break;
                }
            }
        }

        return builder.withEnableDynamicField(enabledDynamic).build();
    }

    @Override
    protected boolean createCollection(Map<String, RawFieldType> rawSchema) {
        CollectionSchemaParam schema = convertSchema(rawSchema);
        param = CreateCollectionParam.newBuilder()
                .withCollectionName(this.collectionName)
                .withSchema(schema)
                .withConsistencyLevel(ConsistencyLevelEnum.EVENTUALLY)
                .build();

        R<Boolean> resp = milvusClient.hasCollection(HasCollectionParam.newBuilder()
                .withCollectionName(param.getCollectionName())
                .withDatabaseName(param.getDatabaseName())
                .build());
        if (resp.getData()) {
            milvusClient.dropCollection(DropCollectionParam.newBuilder()
                    .withCollectionName(param.getCollectionName())
                    .withDatabaseName(param.getDatabaseName())
                    .build());
        }

        milvusClient.createCollection(param);
        return true;
    }

    @Override
    protected boolean createIndex() {
        List<FieldType> fields = param.getFieldTypes();
        for (FieldType field : fields) {
            if (field.getDataType() == DataType.FloatVector) {
                milvusClient.createIndex(CreateIndexParam.newBuilder()
                        .withCollectionName(param.getCollectionName())
                        .withDatabaseName(param.getDatabaseName())
                        .withFieldName(field.getName())
                        .withIndexType(IndexType.AUTOINDEX)
                        .withMetricType(MetricType.COSINE)
                        .withSyncMode(Boolean.TRUE)
                        .build());
            }
        }
        return true;
    }

    private List<JSONObject> convertRows(List<Map<String, Object>> rows) {
        List<FieldType> fieldTypes = param.getFieldTypes();
        List<JSONObject> data = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            JSONObject obj = new JSONObject();
            for (FieldType fieldType : fieldTypes) {
                if (row.containsKey(fieldType.getName())) {
                    obj.put(fieldType.getName(), row.get(fieldType.getName()));
                }
            }

            if (param.isEnableDynamicField() && row.containsKey(Constant.DYNAMIC_FIELD_NAME)) {
                obj.put(Constant.DYNAMIC_FIELD_NAME, row.get(Constant.DYNAMIC_FIELD_NAME));
            }

            data.add(obj);
        }

        return data;
    }

    @Override
    protected boolean insertRows(List<Map<String, Object>> rows) {
        List<JSONObject> data = convertRows(rows);
        R<MutationResult> resp = milvusClient.insert(InsertParam.newBuilder()
                .withCollectionName(param.getCollectionName())
                .withDatabaseName(param.getDatabaseName())
                .withRows(data)
                .build());
        return resp.getStatus() == R.Status.Success.getCode();
    }

    @Override
    protected void postRun() {
        milvusClient.flush(FlushParam.newBuilder().addCollectionName(this.collectionName).build());

        R<GetCollectionStatisticsResponse> statResp = milvusClient.getCollectionStatistics(
                GetCollectionStatisticsParam.newBuilder()
                        .withCollectionName(this.collectionName)
                        .build());
        GetCollStatResponseWrapper wrapper = new GetCollStatResponseWrapper(statResp.getData());
        System.out.println("Collection row count: " + wrapper.getRowCount());

        try {
            while (true) {
                R<GetIndexStateResponse> indexResp = milvusClient.getIndexState(GetIndexStateParam.newBuilder()
                        .withCollectionName(this.collectionName)
                        .build());
                IndexState state = indexResp.getData().getState();
                if (state == IndexState.Finished) {
                    System.out.println("Index done");
                    break;
                }
                System.out.println("Index not yet ready, wait 1 second...");
                TimeUnit.SECONDS.sleep(1L);
            }
        } catch (Exception ignore) {

        }

        milvusClient.close();
    }
}

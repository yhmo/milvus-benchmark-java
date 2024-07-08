package io.milvus.benchmark.milvus;

import io.milvus.Utils;
import io.milvus.param.ConnectParam;

import java.util.Map;

public class MilvusConfig {
    public ConnectParam connectParam;
    public MilvusConfig() {
        init();
    }

    private void init() {
        Map<String, Object> configurations = Utils.readConfigurations();
        Map<String, Object> milvusConfig = (Map<String, Object>)configurations.get("milvus");

        ConnectParam.Builder builder = ConnectParam.newBuilder();
        Object uri = milvusConfig.get("uri");
        Object token = milvusConfig.get("token");
        if (uri != null && token != null) {
            builder.withUri((String)uri).withToken((String)token);
        } else {
            builder.withHost((String)milvusConfig.get("host"));
            builder.withPort((Integer)milvusConfig.get("port"));
        }
        builder.withAuthorization((String)milvusConfig.get("user"), (String)milvusConfig.get("password"));
        connectParam = builder.build();
    }

}

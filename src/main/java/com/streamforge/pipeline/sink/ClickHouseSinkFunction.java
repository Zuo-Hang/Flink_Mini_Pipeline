package com.streamforge.pipeline.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamforge.pipeline.config.ClickHouseConfig;
import com.streamforge.pipeline.model.OrderMetric;
import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ClickHouseSinkFunction extends RichSinkFunction<OrderMetric> {

    private static final Logger LOG = LoggerFactory.getLogger(ClickHouseSinkFunction.class);
    private static final MediaType MEDIA_TYPE = MediaType.parse("application/json");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final ClickHouseConfig config;
    private final List<OrderMetric> buffer = new ArrayList<>();

    private transient OkHttpClient client;

    public ClickHouseSinkFunction(ClickHouseConfig config) {
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) {
        this.client = new OkHttpClient.Builder()
                .callTimeout(java.time.Duration.ofSeconds(config.getTimeoutSeconds()))
                .build();
    }

    @Override
    public void invoke(OrderMetric value, Context context) throws Exception {
        buffer.add(value);
        if (buffer.size() >= config.getBatchSize()) {
            flush();
        }
    }

    @Override
    public void close() throws Exception {
        flush();
        if (client != null) {
            client.connectionPool().evictAll();
        }
    }

    private void flush() throws IOException {
        if (buffer.isEmpty()) {
            return;
        }
        StringBuilder payload = new StringBuilder();
        for (OrderMetric metric : buffer) {
            payload.append(MAPPER.writeValueAsString(metric)).append("\n");
        }
        String query = String.format("INSERT INTO %s.%s FORMAT JSONEachRow", config.getDatabase(), config.getTable());
        Request.Builder builder = new Request.Builder()
                .url(String.format("http://%s:%d/?query=%s", config.getHost(), config.getPort(), query))
                .post(RequestBody.create(payload.toString().getBytes(), MEDIA_TYPE));
        if (config.getUser() != null && !config.getUser().isEmpty()) {
            builder.header("Authorization", Credentials.basic(config.getUser(), config.getPassword() == null ? "" : config.getPassword()));
        }
        try (Response response = client.newCall(builder.build()).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("ClickHouse 写入失败: " + response.message());
            }
            LOG.debug("Inserted {} rows into ClickHouse", buffer.size());
        } finally {
            buffer.clear();
        }
    }
}


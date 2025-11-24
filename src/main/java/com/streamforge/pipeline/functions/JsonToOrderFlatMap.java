package com.streamforge.pipeline.functions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.streamforge.pipeline.model.OrderEvent;
import com.streamforge.pipeline.util.AnomalyLogger;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.logging.Logger;

public class JsonToOrderFlatMap extends RichFlatMapFunction<String, OrderEvent> {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private transient Logger anomalyLogger;

    @Override
    public void open(Configuration parameters) {
        this.anomalyLogger = AnomalyLogger.get();
    }

    @Override
    public void flatMap(String value, Collector<OrderEvent> out) {
        try {
            JsonNode node = MAPPER.readTree(value);
            JsonNode eventIdNode = node.get("event_id");
            if (eventIdNode == null || eventIdNode.asText().isEmpty()) {
                throw new IllegalArgumentException("event_id 缺失");
            }
            String userId = node.hasNonNull("user_id") ? node.get("user_id").asText() : "unknown";
            String storeId = node.hasNonNull("store_id") ? node.get("store_id").asText() : "default";
            double amount = node.hasNonNull("amount") ? node.get("amount").asDouble() : 0.0;
            long eventTime = node.hasNonNull("event_time") ? node.get("event_time").asLong() : 0L;
            OrderEvent event = new OrderEvent(eventIdNode.asText(), userId, storeId, amount, eventTime);
            out.collect(event);
        } catch (Exception e) {
            anomalyLogger.info(() -> "Failed to parse message: " + value + ", err=" + e.getMessage());
        }
    }
}


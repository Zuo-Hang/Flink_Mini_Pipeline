package com.streamforge.pipeline.functions;

import com.streamforge.pipeline.model.OrderEvent;
import com.streamforge.pipeline.model.OrderMetric;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Set;

public class MetricsWindowFunction extends ProcessWindowFunction<OrderEvent, OrderMetric, String, TimeWindow> {

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);

    @Override
    public void process(String key, Context context, Iterable<OrderEvent> elements, Collector<OrderMetric> out) {
        long orderCnt = 0;
        double gmvTotal = 0.0;
        Set<String> users = new HashSet<>();
        for (OrderEvent event : elements) {
            orderCnt++;
            gmvTotal += event.getAmount();
            users.add(event.getUserId());
        }
        OrderMetric metric = new OrderMetric(
                FORMATTER.format(Instant.ofEpochMilli(context.window().getStart())),
                FORMATTER.format(Instant.ofEpochMilli(context.window().getEnd())),
                key,
                orderCnt,
                Math.round(gmvTotal * 100.0) / 100.0,
                users.size()
        );
        out.collect(metric);
    }
}


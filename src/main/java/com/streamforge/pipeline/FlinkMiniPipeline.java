package com.streamforge.pipeline;

import com.streamforge.pipeline.config.ClickHouseConfig;
import com.streamforge.pipeline.config.ConfigLoader;
import com.streamforge.pipeline.config.KafkaClientConfig;
import com.streamforge.pipeline.functions.DeduplicateFunction;
import com.streamforge.pipeline.functions.JsonToOrderFlatMap;
import com.streamforge.pipeline.functions.MetricsWindowFunction;
import com.streamforge.pipeline.model.OrderEvent;
import com.streamforge.pipeline.model.OrderMetric;
import com.streamforge.pipeline.sink.ClickHouseSinkFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public class FlinkMiniPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkMiniPipeline.class);

    public static void main(String[] args) throws Exception {
        Arguments arguments = Arguments.parse(args);
        KafkaClientConfig kafkaConfig = ConfigLoader.loadKafkaConfig(arguments.kafkaConfig);
        ClickHouseConfig clickHouseConfig = ConfigLoader.loadClickHouseConfig(arguments.ckConfig);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(30_000);

        FlinkKafkaConsumer<String> consumer = buildConsumer(kafkaConfig);

        WatermarkStrategy<OrderEvent> watermarkStrategy = WatermarkStrategy
                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((SerializableTimestampAssigner<OrderEvent>) (element, recordTimestamp) -> element.getEventTimeMs());

        DataStream<OrderMetric> metrics = env
                .addSource(consumer)
                .name("kafka_source")
                .flatMap(new JsonToOrderFlatMap())
                .name("json_parser")
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(OrderEvent::getEventId)
                .process(new DeduplicateFunction(Time.hours(1).toMilliseconds()))
                .name("deduplicate")
                .keyBy(OrderEvent::getStoreId)
                .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(30)))
                .process(new MetricsWindowFunction())
                .name("window_metrics");

        metrics.addSink(new ClickHouseSinkFunction(clickHouseConfig)).name("ck_sink");

        LOG.info("Starting StreamForge Pipeline (Java) ...");
        env.execute("StreamForge Pipeline (Java)");
    }

    private static FlinkKafkaConsumer<String> buildConsumer(KafkaClientConfig cfg) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", cfg.getBootstrapServers());
        props.setProperty("group.id", cfg.getGroupId());
        props.setProperty("auto.offset.reset", cfg.getConsumer().getStartFrom());
        props.setProperty("enable.auto.commit", "false");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(cfg.getTopic(), new SimpleStringSchema(), props);
        switch (cfg.getConsumer().getStartFrom()) {
            case "earliest":
                consumer.setStartFromEarliest();
                break;
            case "latest":
                consumer.setStartFromLatest();
                break;
            case "timestamp":
                if (cfg.getConsumer().getTimestampMs() != null) {
                    consumer.setStartFromTimestamp(cfg.getConsumer().getTimestampMs());
                }
                break;
            default:
                consumer.setStartFromLatest();
        }
        return consumer;
    }

    private static class Arguments {
        final String kafkaConfig;
        final String ckConfig;

        Arguments(String kafkaConfig, String ckConfig) {
            this.kafkaConfig = kafkaConfig;
            this.ckConfig = ckConfig;
        }

        static Arguments parse(String[] args) {
            String kafkaPath = null;
            String ckPath = null;
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--kafka-config":
                        kafkaPath = args[++i];
                        break;
                    case "--ck-config":
                        ckPath = args[++i];
                        break;
                    default:
                        // ignore
                }
            }
            if (kafkaPath == null || ckPath == null) {
                throw new IllegalArgumentException("需要提供 --kafka-config 与 --ck-config 参数");
            }
            return new Arguments(kafkaPath, ckPath);
        }
    }
}


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
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
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
        
        // 配置 Checkpoint（精确一次语义的核心）
        configureCheckpoint(env);
        
        // 配置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
            3, // 最多重启3次
            Time.seconds(10) // 每次重启间隔10秒
        ));

        FlinkKafkaConsumer<String> consumer = buildConsumer(kafkaConfig);

        WatermarkStrategy<OrderEvent> watermarkStrategy = WatermarkStrategy
                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((SerializableTimestampAssigner<OrderEvent>) (element, recordTimestamp) -> element.getEventTimeMs());

        DataStream<OrderEvent> events = env
                .addSource(consumer)
                .name("kafka_source")
                .flatMap(new JsonToOrderFlatMap())
                .name("json_parser")
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(OrderEvent::getEventId)
                .process(new DeduplicateFunction(Time.hours(1).toMilliseconds()))
                .name("deduplicate");

        // 窗口聚合，允许 5 秒延迟数据
        OutputTag<OrderEvent> lateEventsTag = new OutputTag<OrderEvent>("late-events") {};
        SingleOutputStreamOperator<OrderMetric> metrics = events
                .keyBy(OrderEvent::getStoreId)
                .window(SlidingEventTimeWindows.of(
                    org.apache.flink.streaming.api.windowing.time.Time.minutes(1), 
                    org.apache.flink.streaming.api.windowing.time.Time.seconds(30)))
                .allowedLateness(org.apache.flink.streaming.api.windowing.time.Time.seconds(5))  // 允许 5 秒延迟数据进入窗口
                .sideOutputLateData(lateEventsTag)  // 超时数据输出到侧输出流
                .process(new MetricsWindowFunction())
                .name("window_metrics");

        // 主流：正常窗口数据写入 ClickHouse
        metrics.addSink(new ClickHouseSinkFunction(clickHouseConfig)).name("ck_sink");

        // 侧输出流：处理延迟数据（可以写入单独的存储或告警）
        DataStream<OrderEvent> lateEvents = metrics.getSideOutput(lateEventsTag);
        lateEvents
                .process(new ProcessFunction<OrderEvent, Void>() {
                    @Override
                    public void processElement(OrderEvent value, ProcessFunction<OrderEvent, Void>.Context ctx, Collector<Void> out) {
                        LOG.warn("检测到延迟事件: event_id={}, event_time={}, current_watermark={}", 
                            value.getEventId(), value.getEventTimeMs(), ctx.timerService().currentWatermark());
                        // 可以在这里实现延迟数据的处理逻辑，如写入单独的存储、发送告警等
                    }
                })
                .name("late_data_handler");

        LOG.info("Starting StreamForge Pipeline (Java) ...");
        env.execute("StreamForge Pipeline (Java)");
    }

    /**
     * 配置 Checkpoint 以实现精确一次语义
     * 关键配置：
     * 1. Checkpoint 模式：EXACTLY_ONCE（精确一次）
     * 2. StateBackend：FsStateBackend（支持大状态，持久化到文件系统）
     * 3. Checkpoint 存储：外部化存储，支持故障恢复
     * 4. 最小间隔：防止 Checkpoint 过于频繁影响性能
     */
    private static void configureCheckpoint(StreamExecutionEnvironment env) {
        // 启用 Checkpoint，间隔 30 秒
        env.enableCheckpointing(30_000);
        
        // 设置 Checkpoint 模式为精确一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        
        // 设置 Checkpoint 超时时间（10 分钟）
        env.getCheckpointConfig().setCheckpointTimeout(600_000);
        
        // 设置两次 Checkpoint 之间的最小间隔（防止过于频繁）
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(10_000);
        
        // 设置最大并发 Checkpoint 数量
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        
        // 配置 Checkpoint 失败时的处理策略
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        
        // 外部化 Checkpoint：作业取消时保留 Checkpoint，用于恢复
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        
        // 配置 StateBackend：使用文件系统 StateBackend（支持大状态，持久化到文件系统）
        // 注意：生产环境应该配置为 HDFS/S3 等分布式存储
        try {
            // 使用文件系统 StateBackend（本地演示）
            // 生产环境应使用：new FsStateBackend("hdfs://namenode:port/flink/checkpoints")
            StateBackend stateBackend = new FsStateBackend("file:///tmp/flink/checkpoints", true);
            env.setStateBackend(stateBackend);
            LOG.info("使用文件系统 StateBackend，Checkpoint 存储路径: file:///tmp/flink/checkpoints");
            
            // 如果需要使用 RocksDB（支持更大的状态），可以这样配置：
            // StateBackend rocksDBBackend = new RocksDBStateBackend("file:///tmp/flink/checkpoints", true);
            // env.setStateBackend(rocksDBBackend);
        } catch (Exception e) {
            LOG.warn("StateBackend 配置失败，使用默认配置: {}", e.getMessage());
        }
    }

    /**
     * 构建 Kafka Consumer，配置精确一次消费模式
     * 关键配置：
     * 1. enable.auto.commit = false：禁用自动提交，由 Flink Checkpoint 管理 offset
     * 2. 使用 FlinkKafkaConsumer 的精确一次模式
     */
    private static FlinkKafkaConsumer<String> buildConsumer(KafkaClientConfig cfg) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", cfg.getBootstrapServers());
        props.setProperty("group.id", cfg.getGroupId());
        props.setProperty("auto.offset.reset", cfg.getConsumer().getStartFrom());
        
        // 精确一次语义的关键配置：禁用自动提交，由 Flink Checkpoint 管理 offset
        props.setProperty("enable.auto.commit", "false");
        
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(cfg.getTopic(), new SimpleStringSchema(), props);
        
        // 设置精确一次消费模式（offset 在 Checkpoint 中提交）
        // 这样在故障恢复时，可以从 Checkpoint 恢复 offset，避免重复消费或丢失数据
        consumer.setCommitOffsetsOnCheckpoints(true);
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


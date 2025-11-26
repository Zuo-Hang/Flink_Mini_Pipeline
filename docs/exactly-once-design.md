# 精确一次语义（Exactly-Once）设计文档

## 概述

本项目实现了端到端的精确一次语义，确保在故障恢复时不会丢失数据也不会重复计算。这是实时数据处理系统的核心要求之一。

## 设计目标

- **数据不丢失**：故障恢复后，所有已处理的数据都能恢复
- **数据不重复**：相同的数据不会被重复处理
- **端到端一致性**：从 Kafka 消费到 ClickHouse 写入，整个链路保证精确一次

## 实现方案

### 1. Kafka 精确一次消费

**关键配置：**
- `enable.auto.commit = false`：禁用自动提交 offset
- `setCommitOffsetsOnCheckpoints(true)`：offset 在 Checkpoint 中提交

**工作原理：**
- Flink 在 Checkpoint 时，将 Kafka offset 作为状态的一部分保存
- 故障恢复时，从 Checkpoint 恢复 offset，从上次保存的位置继续消费
- 避免了重复消费和丢失数据

**代码实现：**
```java
props.setProperty("enable.auto.commit", "false");
consumer.setCommitOffsetsOnCheckpoints(true);
```

### 2. Flink Checkpoint 配置

**关键配置：**
- Checkpoint 模式：`EXACTLY_ONCE`
- StateBackend：`FsStateBackend`（文件系统，支持大状态）
- Checkpoint 间隔：30 秒
- 外部化 Checkpoint：作业取消时保留，用于恢复

**工作原理：**
- Checkpoint 时，所有算子的状态（包括 Kafka offset、去重状态、窗口状态）都会被快照
- 故障恢复时，从最近的 Checkpoint 恢复所有状态
- 保证状态的一致性

**代码实现：**
```java
env.enableCheckpointing(30_000);
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
env.getCheckpointConfig().setExternalizedCheckpointCleanup(
    CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
);
StateBackend stateBackend = new FsStateBackend("file:///tmp/flink/checkpoints", true);
env.setStateBackend(stateBackend);
```

### 3. ClickHouse 幂等写入

**设计思路：**
- 使用 `ReplacingMergeTree` 引擎，基于 `(window_start, store_id)` 作为唯一键
- 相同窗口和店铺的数据会被自动去重
- 即使重复写入，最终结果也是一致的

**表结构设计：**
```sql
ENGINE = ReplacingMergeTree(update_time)
ORDER BY (window_start, store_id)  -- 唯一键
```

**工作原理：**
- 如果 Checkpoint 失败回滚，可能重复写入相同的数据
- 但通过 `ReplacingMergeTree` 的去重机制，最终只会保留一条数据
- 实现了最终一致性

## 精确一次语义的保证

### 场景 1：正常处理
- Kafka offset 正常提交
- 状态正常保存
- ClickHouse 正常写入
- **结果**：数据不丢失、不重复

### 场景 2：Checkpoint 失败
- Checkpoint 失败，状态未保存
- 作业回滚到上一个 Checkpoint
- Kafka offset 回滚，重新消费
- ClickHouse 可能重复写入，但通过 `ReplacingMergeTree` 去重
- **结果**：数据不丢失，可能重复但最终一致

### 场景 3：作业崩溃
- 作业从最近的 Checkpoint 恢复
- Kafka offset 从 Checkpoint 恢复，继续消费
- 状态从 Checkpoint 恢复，继续处理
- **结果**：数据不丢失、不重复

## 性能考虑

### Checkpoint 频率
- **间隔 30 秒**：平衡了容错能力和性能
- 太频繁：影响吞吐量
- 太稀疏：故障恢复时可能丢失更多数据

### StateBackend 选择
- **FsStateBackend**：适合中等规模状态
- **RocksDBStateBackend**：适合大规模状态（可以切换）
- 生产环境应使用 HDFS/S3 等分布式存储

## 局限性

1. **最终一致性**：ClickHouse 的去重是异步的，不是强一致性
2. **Checkpoint 开销**：Checkpoint 会暂停处理，影响吞吐量
3. **存储依赖**：需要可靠的 Checkpoint 存储（HDFS/S3）

## 总结

通过 Kafka 精确一次消费 + Flink Checkpoint + ClickHouse 幂等写入，实现了端到端的精确一次语义。这是实时数据处理系统的核心能力，确保了数据的准确性和一致性。


# StreamForge Pipeline (Flink Mini Pipeline)

一个可用于端到端演示与面试讲解的实时数据链路示例：`Kafka → Flink → ClickHouse`。项目聚焦于**实时订单指标**的计算，覆盖数据生产、流式计算、指标落地、异常回溯等关键环节。


## ✨ 项目亮点
- 15 分钟可讲清楚：链路、算子、窗口、写入、回溯。
- PyFlink 编写算子逻辑，包含 map / flat_map / window aggregate。
- Kafka 与 ClickHouse 通过 Docker Compose 一键拉起，便于本地演示。
- 提供模拟数据脚本、ClickHouse 建表脚本与回溯示例。


## 🧱 目录结构
```
flink-mini-pipeline/
├── src/main/java/com/streamforge/pipeline/
│   ├── FlinkMiniPipeline.java  # Java DataStream 主作业
│   ├── config/…                # Kafka & ClickHouse 配置读取
│   ├── functions/…             # 算子实现
│   └── sink/…                  # ClickHouse HTTP Sink
├── config/
│   ├── kafka_config.yaml   # Kafka topic / producer 配置
│   └── clickhouse_config.yaml
├── scripts/
│   ├── produce_data.py     # 模拟订单事件写入 Kafka
│   └── create_ck_table.sql # ClickHouse 建表脚本
├── docker/
│   └── docker-compose.yml  # Kafka + Zookeeper + ClickHouse + Flink
└── README.md
```


## 🔄 数据流概述
1. `scripts/produce_data.py` 以 JSON 形式持续生成订单事件（含 event_time、amount、user_id 等字段），写入 Kafka `orders_stream`。
2. `src/main/python/flink_job.py` 从 Kafka 消费事件，执行：
   - 数据清洗：JSON 解析、字段补全、重复事件过滤。
   - 滑动窗口：每 1 分钟滑动 30 秒统计订单数、总金额、独立用户数。
   - 异常旁路：写入本地日志文件，支持回溯。
   - 指标落库：通过 ClickHouse HTTP 接口落地聚合结果。
3. ClickHouse 以 `event_window_start` 作为分区字段，为实时看板/查询提供支撑。


## 🚀 快速开始
1. **启动依赖**
   ```bash
   cd docker
   docker compose up -d
   ```
2. **准备 ClickHouse 表**
   ```bash
   clickhouse-client --host localhost --multiquery < scripts/create_ck_table.sql
   ```
3. **运行数据生产脚本**
   ```bash
   python scripts/produce_data.py --rate 50
   ```
4. **提交 Flink 作业（Java）**
   ```bash
   mvn -T4C -DskipTests clean package
   flink run -c com.streamforge.pipeline.FlinkMiniPipeline \
     target/flink-mini-pipeline-0.1.0-shaded.jar \
     --kafka-config config/kafka_config.yaml \
     --ck-config config/clickhouse_config.yaml
   ```


## 🧠 面试讲解要点
- **链路讲解**：Kafka → Flink → ClickHouse，强调窗口指标落地与查询。
- **算子设计**：map/flat_map 做清洗与补全，key_by + window + aggregate 做指标，process function 负责异常旁路。
- **状态与容错**：演示 RocksDB/文件系统 checkpoint 配置思路，说明乱序/延迟处理可通过 watermark + allowed lateness 扩展。
- **回溯机制**：异常数据落地日志，触发脚本重放指定时间窗口的 Kafka 分区，实现“重算”演示。


## 📦 可扩展方向
- 增加 Watermark 与延迟处理示例，讨论乱序场景。
- 引入 Flink SQL Table API，提供同逻辑的 SQL 版本。
- 扩展 ClickHouse 维度（如品类、城市），展示多指标聚合。
- 集成数据质量校验框架（Great Expectations/自研脚本）。


## 📝 参考指标定义
| 指标 | 说明 | 实现方式 |
| --- | --- | --- |
| `order_cnt` | 窗口内订单条数 | `AggregateFunction` 计数 |
| `gmv_total` | 订单金额合计 | `AggregateFunction` sum |
| `user_cnt` | 独立下单用户数 | 在状态中维护 set，窗口关闭输出大小 |


## 🧪 本地演示脚本
- `scripts/produce_data.py`：支持自定义速率、随机异常。
- `scripts/create_ck_table.sql`：单节点 ClickHouse 表结构。
- `flink_job.py`：在 IDE 中断点调试，或提交到本地 Flink 集群。


## 🧯 错误处理 & 回溯
- 解析失败或字段缺失 → 写入 `logs/anomalies.log`。
- 回溯流程：调整 `produce_data.py` 读取历史文件 → 重放到 Kafka → Flink 自动重新计算窗口结果。

> ✅ 通过该示例，可在 30 分钟内完成演示，并深入讨论算子、状态、容错与优化。



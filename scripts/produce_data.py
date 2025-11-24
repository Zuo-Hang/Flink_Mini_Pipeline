import argparse
import json
import random
import string
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Tuple

from kafka import KafkaProducer
import yaml


@dataclass
class KafkaConfig:
    bootstrap_servers: str
    topic: str
    producer: dict


def load_kafka_config(path: str) -> KafkaConfig:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return KafkaConfig(
        bootstrap_servers=data["bootstrap_servers"],
        topic=data["topic"],
        producer=data.get("producer", {}),
    )


def random_id(prefix: str = "evt") -> str:
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f"{prefix}_{suffix}"


def build_producer(config_path: str) -> Tuple[KafkaProducer, str]:
    kafka_cfg = load_kafka_config(config_path)
    producer = KafkaProducer(
        bootstrap_servers=kafka_cfg.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=kafka_cfg.producer.get("linger_ms", 50),
        batch_size=kafka_cfg.producer.get("batch_size", 16384),
        retries=kafka_cfg.producer.get("retries", 3),
        acks=kafka_cfg.producer.get("acks", "all"),
    )
    return producer, kafka_cfg.topic


def produce(args):
    producer, topic = build_producer(args.kafka_config)
    end_time = time.time() + args.duration if args.duration > 0 else float("inf")

    try:
        while time.time() < end_time:
            payload = generate_order_event(args.allow_anomaly)
            producer.send(topic, payload)
            if args.verbose:
                print(f"[{datetime.utcnow()}] send -> {payload}")
            time.sleep(1 / args.rate)
    finally:
        producer.flush()
        producer.close()


def generate_order_event(allow_anomaly: bool):
    base = {
        "event_id": random_id("order"),
        "user_id": random.choice([f"user_{i}" for i in range(20)]),
        "store_id": random.choice(["store_north", "store_south", "store_east"]),
        "amount": round(random.uniform(5.0, 200.0), 2),
        "event_time": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
    }
    if allow_anomaly and random.random() < 0.05:
        base.pop("event_id")
    return base


def parse_args():
    parser = argparse.ArgumentParser(description="Kafka 数据生产脚本")
    parser.add_argument("--kafka-config", default="config/kafka_config.yaml")
    parser.add_argument("--rate", type=int, default=20, help="每秒写入条数")
    parser.add_argument("--duration", type=int, default=0, help="运行秒数，0 表示一直运行")
    parser.add_argument("--allow-anomaly", action="store_true", help="是否随机产生异常事件")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    produce(parse_args())


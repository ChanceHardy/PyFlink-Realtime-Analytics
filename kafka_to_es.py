# kafka_to_es.py

import json
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch

# --- 配置 ---
KAFKA_TOPIC = 'flink_output_counts'
KAFKA_SERVER = 'localhost:9092'
ES_SERVER = 'localhost:9200'
ES_INDEX = 'flink_event_counts'

# --- 定义我们期望的 Elasticsearch 映射 ---
INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "event_type": { "type": "keyword" },
            "count": { "type": "integer" },
            "@timestamp": { "type": "date" }
        }
    }
}

print("正在连接 Elasticsearch...")
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])
print("连接成功！")

print(f"正在从 Kafka 主题 '{KAFKA_TOPIC}' 消费数据...")
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    api_version=(0, 10, 2),
    value_deserializer=lambda v: v.decode('utf-8')
)

# --- 自动删除并用正确的映射重建索引 ---
if es.indices.exists(index=ES_INDEX):
    es.indices.delete(index=ES_INDEX)
    print(f"删除了旧的 Elasticsearch 索引 '{ES_INDEX}'")

es.indices.create(index=ES_INDEX, body=INDEX_MAPPING)
print(f"使用正确的映射创建了新的 Elasticsearch 索引 '{ES_INDEX}'")

# --- 主循环 ---
for message in consumer:
    try:
        doc = json.loads(message.value)
        es.index(index=ES_INDEX, document=doc)
        print(f"成功写入 ES: {doc}")
    except Exception as e:
        print(f"写入失败: {e}")
# flink_consumer_to_console.py

import os
import pathlib
from pyflink.common import WatermarkStrategy, Types
# ↓↓↓ 1. 在这里多导入一个 SimpleStringSchema ↓↓↓
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'user_behavior_log'

def run_flink_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    
    kafka_connector_jar = "flink-connector-kafka-3.0.1-1.17.jar"
    kafka_clients_jar = "kafka-clients-3.4.0.jar"

    script_dir = os.path.dirname(os.path.realpath(__file__))
    jars_dir = os.path.join(script_dir, "jars")
    
    kafka_connector_uri = pathlib.Path(os.path.join(jars_dir, kafka_connector_jar)).as_uri()
    kafka_clients_uri = pathlib.Path(os.path.join(jars_dir, kafka_clients_jar)).as_uri()

    env.add_jars(kafka_connector_uri, kafka_clients_uri)
    
    env.set_parallelism(1)

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
        .set_topics(KAFKA_TOPIC) \
        .set_group_id("flink_consumer_group_console") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    data_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.for_monotonous_timestamps(),
        "Kafka_Source"
    )

    data_stream.print()
    
    print("Flink 作业已提交，正在等待从 Kafka 接收数据...")
    env.execute("Kafka to Console Flink Job")


if __name__ == '__main__':
    run_flink_job()
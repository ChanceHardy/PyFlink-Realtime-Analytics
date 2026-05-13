# flink_to_kafka.py (完整、格式正确版)

import os
import json
import pathlib
from datetime import datetime
from pyflink.common import WatermarkStrategy, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors import FlinkKafkaProducer
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import TumblingProcessingTimeWindows, Time
from pyflink.datastream.functions import ProcessWindowFunction

def run_flink_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)
    
    # --- JAR 依赖配置 (保持不变) ---
    kafka_connector_jar = "flink-connector-kafka-3.0.1-1.17.jar"
    kafka_clients_jar = "kafka-clients-3.4.0.jar"
    script_dir = os.path.dirname(os.path.realpath(__file__))
    jars_dir = os.path.join(script_dir, "jars")
    kafka_connector_uri = pathlib.Path(os.path.join(jars_dir, kafka_connector_jar)).as_uri()
    kafka_clients_uri = pathlib.Path(os.path.join(jars_dir, kafka_clients_jar)).as_uri()
    env.add_jars(kafka_connector_uri, kafka_clients_uri)
    env.set_parallelism(1)

    # --- Source & Transformations (保持不变) ---
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers('localhost:9092') \
        .set_topics('user_behavior_log') \
        .set_group_id("flink_kafka_to_kafka_group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    data_stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka_Source")
    
    def parse_and_stringify_json(msg):
        data = json.loads(msg)
        return {k: str(v) for k, v in data.items()}
    parsed_stream = data_stream.map(
        parse_and_stringify_json, 
        output_type=Types.MAP(Types.STRING(), Types.STRING())
    )
    
    class EventCounter(ProcessWindowFunction):
        def process(self, key, context, elements):
            # 【关键修改】使用 utcfromtimestamp 并添加 'Z' 来生成标准的 UTC 时间字符串
            utc_timestamp_str = datetime.utcfromtimestamp(context.window().end / 1000).isoformat() + "Z"
            
            doc = {
                "event_type": key,
                "count": len(list(elements)),
                "@timestamp": utc_timestamp_str  # 使用修正后的时间字符串
            }
            yield json.dumps(doc)
            
        def get_result_type(self):
            return Types.STRING()

    result_stream = parsed_stream.key_by(lambda e: e['event_type']) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) \
        .process(EventCounter(), output_type=Types.STRING())

    # --- Sink: 使用 FlinkKafkaProducer (保持不变) ---
    output_topic = 'flink_output_counts'
    kafka_server = 'localhost:9092'

    kafka_producer = FlinkKafkaProducer(
        topic=output_topic,
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': kafka_server}
    )
        
    result_stream.add_sink(kafka_producer).name("Kafka Sink")

    print("Flink 作业已提交，开始将计算结果写入 Kafka 'flink_output_counts' 主题...")
    env.execute("Real-time window count to Kafka")

if __name__ == '__main__':
    run_flink_job()
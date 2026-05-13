# flink_window_count.py

import os
import json
import pathlib
from pyflink.common import WatermarkStrategy, Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import TumblingProcessingTimeWindows, Time
# ↓↓↓ 1. 导入 ProcessWindowFunction ↓↓↓
from pyflink.datastream.functions import ProcessWindowFunction

# ↓↓↓ 2. 定义一个继承自 ProcessWindowFunction 的类 ↓↓↓
class EventCounter(ProcessWindowFunction):
    
    # Flink 会调用这个 process 方法来处理窗口中的数据
    def process(self, key, context, elements):
        """
        :param key: 窗口的 key，也就是我们 key_by 的字段 ('event_type')
        :param context: 窗口的上下文信息，我们这里用不到
        :param elements: 窗口内所有的元素，是一个可迭代对象
        :return: 返回一个包含 (key, count) 元组的列表
        """
        # 计算窗口内元素的数量
        count = len(list(elements))
        
        # 使用 yield 关键字返回结果，这是 PyFlink 推荐的方式
        yield (key, count)

    # Flink 需要知道这个函数返回的数据类型
    def get_result_type(self):
        return Types.TUPLE([Types.STRING(), Types.INT()])


def run_flink_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)
    
    # --- JAR 依赖配置 ---
    kafka_connector_jar = "flink-connector-kafka-3.0.1-1.17.jar"
    kafka_clients_jar = "kafka-clients-3.4.0.jar"
    script_dir = os.path.dirname(os.path.realpath(__file__))
    jars_dir = os.path.join(script_dir, "jars")
    kafka_connector_uri = pathlib.Path(os.path.join(jars_dir, kafka_connector_jar)).as_uri()
    kafka_clients_uri = pathlib.Path(os.path.join(jars_dir, kafka_clients_jar)).as_uri()
    env.add_jars(kafka_connector_uri, kafka_clients_uri)
    env.set_parallelism(1)

    # --- 1. 定义 Kafka Source ---
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers('localhost:9092') \
        .set_topics('user_behavior_log') \
        .set_group_id("flink_window_count_group") \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    
    data_stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka_Source")

    # --- 2. 核心处理逻辑 ---
    def parse_and_stringify_json(msg):
        data = json.loads(msg)
        return {k: str(v) for k, v in data.items()}

    parsed_stream = data_stream.map(
        parse_and_stringify_json, 
        output_type=Types.MAP(Types.STRING(), Types.STRING())
    )

    keyed_stream = parsed_stream.key_by(lambda e: e['event_type'])

    windowed_stream = keyed_stream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    
    # ↓↓↓ 3. 在 .process() 方法中，实例化我们定义的类 ↓↓↓
    result_stream = windowed_stream.process(EventCounter())


    # --- 3. 定义 Sink ---
    result_stream.print()
    
    print("Flink 作业已提交，开始进行10秒窗口计数...")
    env.execute("Real-time Event Count")

if __name__ == '__main__':
    run_flink_job()
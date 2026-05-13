# producer.py

import json
import time
from kafka import KafkaProducer
import random

# Kafka 服务器地址
bootstrap_servers = 'localhost:9092'

# 创建一个 Kafka 生产者实例
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    # ↓↓↓ 新增的关键参数 ↓↓↓
    # 明确指定 API 版本，以避免客户端与服务器之间的版本自动协商问题
    api_version=(0, 10, 2),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 模拟的用户行为事件类型
event_types = ['page_view', 'add_to_cart', 'purchase', 'click_ad']
# 模拟的页面ID
page_ids = ['/home', '/product/123', '/cart', '/checkout', '/ad/abc']

print("开始向 Kafka 发送模拟用户行为数据...")

try:
    user_id_counter = 0
    while True:
        user_id_counter += 1
        message = {
            'user_id': f'user_{user_id_counter % 50}',
            'event_type': random.choice(event_types),
            'page_id': random.choice(page_ids),
            'timestamp': int(time.time() * 1000)
        }
        
        topic = 'user_behavior_log'
        producer.send(topic, message)
        
        print(f"发送成功: {message}")
        
        time.sleep(1)

except KeyboardInterrupt:
    print("\n停止发送数据。")

finally:
    producer.flush()
    producer.close()
    print("生产者已关闭。")
PyFlink-Realtime-Analytics
A real-time user behavior analysis pipeline built with PyFlink, Kafka, and Elasticsearch.
基于 PyFlink、Kafka 和 Elasticsearch 构建的实时用户行为分析流水线。






docker-compose up -d

python producer.py

python flink_to_kafka.py

python kafka_to_es.py

go to http://localhost:3000

docker-compose down

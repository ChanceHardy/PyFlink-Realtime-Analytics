
docker-compose up -d

python producer.py

python flink_to_kafka.py

python kafka_to_es.py

go to http://localhost:3000

docker-compose down
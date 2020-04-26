#/bin/bash

docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --create --zookeeper localhost:22181 --replication-factor 3 --partitions 3 --topic order-request

docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --create --zookeeper localhost:22181 --replication-factor 3 --partitions 3 --topic order-processed
docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --create --zookeeper localhost:22181 --replication-factor 3 --partitions 3 --topic customer --config cleanup.policy=compact,delete
docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --create --zookeeper localhost:22181 --replication-factor 3 --partitions 3 --topic stock --config cleanup.policy=compact,delete

docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --zookeeper localhost:22181 --list

#docker run --rm  -it --net=host confluentinc/cp-kafka:latest kafka-console-producer --broker-list localhost:9092 --topic order-request --property "parse.key=true" --property "key.separator=:"

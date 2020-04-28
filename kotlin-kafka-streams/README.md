# Kafka Streams and Kotlin

## Getting up and Running

Start up the Kafka and Zookeeper cluster. Three nodes so need at least two up.

`docker-compose up`

Create the topic for the 

```shell script
docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --create --zookeeper localhost:22181 --replication-factor 3 --partitions 3 --topic order-request
```

```shell script
docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --create --zookeeper localhost:22181 --replication-factor 3 --partitions 3 --topic order-processed
```
Create the table topics and they are required to be `compact` to work as tables
```shell script
docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --create --zookeeper localhost:22181 --replication-factor 3 --partitions 3 --topic customer --config cleanup.policy=compact,delete
```

```shell script
docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --create --zookeeper localhost:22181 --replication-factor 3 --partitions 3 --topic stock --config cleanup.policy=compact,delete
```

```shell script
docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --zookeeper localhost:22181 --list
```

```shell script
docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-console-producer --broker-list localhost:9092 --topic order-request --property "parse.key=true" --property "key.separator=:"
```


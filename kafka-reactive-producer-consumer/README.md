# Kafka Reactive Producer Consumer Example

## Running the Example


Start up Kafka and Zookeeper:

`docker-compose up -d`

Create the required topic in Kafka:

`docker exec reactive-kafka-example-broker kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic example-topic`

Check the topic was created:

`docker exec reactive-kafka-example-broker kafka-topics --zookeeper zookeeper:2181 --list`

Produce some example data to the topic and with the Spring Boot Kafka Reactive App Running it will log the consumption of these messages:

`docker exec -it reactive-kafka-example-broker kafka-console-producer --broker-list broker:9092 --topic example-consumer-topic`

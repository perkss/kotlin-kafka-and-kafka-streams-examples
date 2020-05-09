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

### Populate Data

First we can populate a customer in the customer topic that will populate the KTable for customer, note the 
keys are kept the same between the examples to enable the streaming join with the KTable.

Start the KafkaAvroConsoleProducer and pass the key value pair in to send a `Customer`.
```shell script
docker run --rm  -it --net=host confluentinc/cp-schema-registry:latest kafka-avro-console-producer --broker-list localhost:9092 --topic customer --property "parse.key=true" --property "key.separator=:" --property key.serializer=org.apache.kafka.common.serialization.StringSerializer --property value.schema='{ "namespace": "com.perkss", "type": "record", "name": "Customer", "fields": [ { "name": "id", "type": { "type": "string", "avro.java.string": "String" } }, { "name": "name", "type": { "type": "string", "avro.java.string": "String" } }, { "name": "city", "type": { "type": "string", "avro.java.string": "String" } } ] }'
```

```shell script
1:{"id": "1", "name": "perkss", "city": "london"}
```

View the schema registered in the SchemaRegistry
```shell script
http://0.0.0.0:8081/subjects/customer-value/versions/1
```

Now send a `OrderRequest` which is an event to join with the `Customer` table
```shell script
docker run --rm  -it --net=host confluentinc/cp-schema-registry:latest kafka-avro-console-producer --broker-list localhost:9092 --topic order-request --property "parse.key=true" --property "key.separator=:" --property key.serializer=org.apache.kafka.common.serialization.StringSerializer --property value.schema='{ "namespace": "com.perkss.order.model", "type": "record", "name": "OrderRequested", "fields": [ { "name": "id", "type": { "type": "string", "avro.java.string": "String" } }, { "name": "product_id", "type": { "type": "string", "avro.java.string": "String" } } ] }'
```

```shell script
1:{"id": "1", "product_id": "abc"}
```

```shell script
http://0.0.0.0:8081/subjects/order-request-value/versions/1
```
You can consume the message written using the following console consumer that references the schema registry.

```shell script
docker run --rm  -it --net=host confluentinc/cp-schema-registry:latest kafka-avro-console-consumer --topic order-request --bootstrap-server localhost:9092 --property schema.registry.url="http://0.0.0.0:8081" --from-beginning
```

```shell script
docker run --rm  -it --net=host confluentinc/cp-schema-registry:latest kafka-avro-console-consumer --topic customer --bootstrap-server localhost:9092 --property schema.registry.url="http://0.0.0.0:8081" --from-beginning
```

## Tescontainers Integration Tests

`StreamIntegrationTest` uses [Testcontainers]("https://www.testcontainers.org/") to fire up a running instance of Kafka and Schema Registry and runs our application to drop messages on Kafka process them and read the output. Check it out a very powerful example.






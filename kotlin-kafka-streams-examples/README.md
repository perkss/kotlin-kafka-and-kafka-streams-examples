# Kafka Streams and Kotlin

This module formed of two parts:

1) A fully fledged Kafka Streams Spring Boot application providing an ordering system which Avro, joins and output.

2) Example package containing examples
   for [Windowing, Aggregations and Joining](https://github.com/perkss/kotlin-kafka-and-kafka-streams-examples/tree/master/kotlin-kafka-streams-examples/src/main/kotlin/com/perkss/kafka/reactive/examples)
   .

## Getting up and Running Order Processing Topology

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

First we can populate a customer in the customer topic that will populate the KTable for customer, note the keys are
kept the same between the examples to enable the streaming join with the KTable.

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

## Getting up and Running Boostrap Semantics Topology

`docker-compose up`

Create the topic for the

```shell script
docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --create --zookeeper localhost:22181 --replication-factor 3 --partitions 3 --topic name
docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --alter --zookeeper localhost:22181 --topic name --config cleanup.policy=compact
docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --describe --zookeeper localhost:22181 --topic name 
```

```shell script
docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --create --zookeeper localhost:22181 --replication-factor 3 --partitions 3 --topic name-formatted
```

```shell
docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-console-consumer --bootstrap-server localhost:9092 --topic namedocker run --rm  --net=host confluentinc/cp-kafka:latest kafka-console-consumer --bootstrap-server localhost:9093 --topic name --property print.key=true --from-beginning
docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-console-consumer --bootstrap-server localhost:9093 --topic name-formatted --property print.key=true --from-beginning
```

```shell
docker exec -it kafka-3 kafka-console-producer --broker-list kafka-2:29092  --topic name --property "parse.key=true" --property "key.separator=:"
```

### Test

For the first test we will run just a KTable that consumes the messages off a compacted topic after two messages with
the same key have been placed on a topic.

```shell
Topic: name	PartitionCount: 3	ReplicationFactor: 3	Configs: cleanup.policy=compact
	Topic: name	Partition: 0	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2
	Topic: name	Partition: 1	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3
	Topic: name	Partition: 2	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1
```

```shell
 streamsBuilder
            .table("name", Consumed.with(Serdes.String(), Serdes.String()))
            .toStream()
            .peek { key, value ->
                logger.info("Processing {}, {}", key, value)
            }
            .to("name-formatted")
```

Put two messages on the topic with the same key

```shell
tom	perks
tom matthews
```

If you run the application now as expected it will process both messages.

Now lets add a globaltable into the mix and join on it. This should only join the latest values.

```shell
docker exec -it kafka-3 kafka-streams-application-reset --application-id OrderProcessing \
                                      --input-topics name \
                                      --bootstrap-servers kafka-1:29091,kafka-2:29092,kafka-3:29093 \
                                      --zookeeper zookeeper-1:22181,zookeeper-2:22182,zookeeper-3:22183
```

```shell
        val nameKTable = streamsBuilder
            .table("name", Consumed.with(Serdes.String(), Serdes.String()))

        nameKTable
            .toStream()
            .peek { key, value ->
                logger.info("Processing {}, {}", key, value)
            }
            .join(nameKTable, ValueJoiner { value1, value2 ->
                logger.info("Joining the Stream Name {} to the KTable Name {}", value1, value2)
                value2
            }, Joined.with(Serdes.String(), Serdes.String(), Serdes.String()))
            .to("name-formatted", Produced.with(Serdes.String(), Serdes.String()))
```

Now if we (inner) join the stream to the table itself and we put two messages

```shell
stuart:c
stuart:d
max:a
stuart:e
```

We now get a result of

```shell
stuart:e
```

Now if we left join the stream to the table itself and we put two messages

```shell
perkss:a
perkss:b
sam:a
perkss:c
```

```shell
sam:a
perkss:c
```

You cannot join a global k table on it self
as `Invalid topology: Topic name has already been registered by another source.`

If we were to rekey and join with a different key how are the semantics well let see

```shell
  nameKTable
            .toStream()
            .peek { key, value ->
                logger.info("Processing {}, {}", key, value)
            }
            .selectKey { key, value ->
                val re = Regex("[^A-Za-z0-9 ]")
                re.replace(value, "")
            }
            .join(nameKTable, ValueJoiner { value1, value2 ->
                logger.info("Joining the Stream Name {} to the KTable Name {}", value1, value2)
                value2
            }, Joined.with(Serdes.String(), Serdes.String(), Serdes.String()))
            .to("name-formatted", Produced.with(Serdes.String(), Serdes.String()))
```

```shell
sarah:mark1
mark:sarah1
sarah:mark2
sarah:mark3
mark:sarah2
```

Results in

```shell
Processing sarah, mark3
Processing mark, sarah2

Joining the Stream Name mark3 to the KTable Name sarah2
Joining the Stream Name sarah2 to the KTable Name mark3


OutputTopic > 
sarah2
mark3
```

Therefore we can see that using the KTable and joining with itself in this simple example will only take the latest
value when processing the stream. To guarantee this we could even check the message timestamps if the joined version is
newer use that, or drop the message and wait for the new version to come in.

## Tescontainers Integration Tests

Required Docker to be running.

`StreamIntegrationTest` uses [Testcontainers](https://www.testcontainers.org/) to fire up a running instance of Kafka
and Schema Registry and runs our application to drop messages on Kafka process them and read the output. Check it out a
very powerful example.

## Examples Understanding KStream Windowing

TODO a table of each event, event timestamp, the window its in and the  



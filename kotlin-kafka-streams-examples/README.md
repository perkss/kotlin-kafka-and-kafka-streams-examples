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
docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-console-consumer --bootstrap-server localhost:9093 --topic name --property print.key=true --from-beginning
docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-console-consumer --bootstrap-server localhost:9093 --topic name-formatted --property print.key=true --from-beginning
```

```shell
docker exec -it kafka-3 kafka-console-producer --broker-list kafka-2:29092  --topic name --property "parse.key=true" --property "key.separator=:"
```

### Test semantics

These tests will use the standard properties for cache and buffering. Later we will run the same tests with these turned
off as these will compact the data in memory which may result in different results to with them on a reference
on [memory management](https://docs.confluent.io/platform/current/streams/developer-guide/memory-mgmt.html).

I expect that these initial tests with buffers and cache enabled will compact the data for us and only show the last
key.

For the first test we will run just a KTable that consumes the messages off a compacted topic after two messages with
the same key have been placed on a topic. I would expect that this topology will process all messages on start up
including duplicate keys so we see the full history following streaming semantics.

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

Put two messages on the `name` topic with the same key when the application is stopped.

```shell
tom:perks
tom:matthews
```

```shell
Processing tom, perks
Processing tom, matthew
```

Then run the application, as expected it will process both messages.

```shell
docker exec -it kafka-3 kafka-streams-application-reset --application-id OrderProcessing \
                                      --input-topics name \
                                      --bootstrap-servers kafka-1:29091,kafka-2:29092,kafka-3:29093 \
                                      --zookeeper zookeeper-1:22181,zookeeper-2:22182,zookeeper-3:22183
```

Now let's add a join to itself using the KTable.

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

Now if we (inner) join the stream to the table and send these messages and then start the application up.

```shell
zara:a
zara:b
zara:c
paul:a
```

We now get a result of processing just the last for a key. Interestingly the last message is processed first, most
likely due to the compaction and partitioning.

```shell
Processing paul, a
Joining the Stream Name a to the KTable Name a
Processing zara, c
Joining the Stream Name c to the KTable Name c
```

Now if we left join the stream to the table itself and we put two messages and start up.

```shell
zara:d
zara:e
zara:f
paul:b
```

As expected a left join makes no difference same result as before.

```shell
Processing paul, b
Joining the Stream Name b to the KTable Name b
Processing zara, f
Joining the Stream Name f to the KTable Name f
```

Now lets drop a live message onto the KTable backed by topic `name`

```shell
paul:c
```

This results in:

```shell
Processing paul, c
Joining the Stream Name c to the KTable Name c
```

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

Put these messages onto the compact topic `name` whilst the application is down.

```shell
sarah:mark1
mark:sarah1
sarah:mark2
sarah:mark3
mark:sarah2
```

Results are that we take the latest value like above of the tables and only process that on start up.

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

### Join another table

```shell script
docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --create --zookeeper localhost:22181 --replication-factor 3 --partitions 3 --topic first-name
docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --alter --zookeeper localhost:22181 --topic first-name --config cleanup.policy=compact
docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --describe --zookeeper localhost:22181 --topic first-name 

docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --create --zookeeper localhost:22181 --replication-factor 3 --partitions 3 --topic last-name
docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --alter --zookeeper localhost:22181 --topic last-name --config cleanup.policy=compact
docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --describe --zookeeper localhost:22181 --topic last-name 

docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --create --zookeeper localhost:22181 --replication-factor 3 --partitions 3 --topic joined-name
```

Lets populate the topics before starting the application

```shell
docker exec -it kafka-3 kafka-console-producer --broker-list kafka-2:29092  --topic first-name --property "parse.key=true" --property "key.separator=:"

1:tom
1:matthew
2:mark
```

```shell
docker exec -it kafka-3 kafka-console-producer --broker-list kafka-2:29092  --topic last-name --property "parse.key=true" --property "key.separator=:"

1:banks
2:pears
2:sanders
```

This results in processing all three messages on the stream but no joins successful. Behaviour falls in line with it not
waiting to populate the table and streaming all messages. The timestamps are not matched as we send the table last-name
topic after the streaming messages so they are not joined.

```shell
Processing 2, mark
Processing 1, tom
Processing 1, matthew
```

```shell
 nameKTable
            .toStream()
            .peek { key, value ->
                logger.info("Processing {}, {}", key, value)
            }
            .join(lastNameKTable, ValueJoiner { value1, value2 ->
                logger.info("Joining the Stream First Name {} to the KTable Last Name {}", value1, value2)
                "$value1 $value2"
            }, Joined.with(Serdes.String(), Serdes.String(), Serdes.String()))
            .to("joined-name", Produced.with(Serdes.String(), Serdes.String()))

```

If we send a last name then a first name like so

Sending on the `last-name` topic.

```shell
3:last
```

Then send on the `first-name` topic.

```shell
3:first
```

Result we get the join successful.

```shell
Processing 3, first
Joining the Stream First Name first to the KTable Last Name last
```

This is due to the timing semantics of KTable where they are event times so this case the default Kafka broker event
time.

Lets put another first name with the same key.

Now lets do it with a GlobalKTable I would expect the GlobalKtable to pause execution until populated and then join
successfully but still stream all keys.

```shell
3:first2
```

Again it joins

```shell
Processing 3, first2
Joining the Stream First Name first2 to the KTable Last Name last
```

If a late message came it would not join if its timestamp was before the table message timestamp. Here the timestamps
are related.

```shell
  val nameKTable = streamsBuilder
            .table("first-name", Consumed.with(Serdes.String(), Serdes.String()))

        val lastNameKTable = streamsBuilder
            .globalTable("last-name", Consumed.with(Serdes.String(), Serdes.String()))

        nameKTable
            .toStream()
            .peek { key, value ->
                logger.info("Processing {}, {}", key, value)
            }
            .join(
                lastNameKTable,
                KeyValueMapper<String, String, String> { key, value -> key },
                ValueJoiner { value1: String, value2: String ->
                    logger.info("Joining the Stream First Name {} to the KTable Last Name {}", value1, value2)
                    "$value1 $value2"
                }, Named.`as`("global")
            )
            .to("joined-name", Produced.with(Serdes.String(), Serdes.String()))

```

```shell
docker exec -it kafka-3 kafka-console-producer --broker-list kafka-2:29092  --topic first-name --property "parse.key=true" --property "key.separator=:"

1:peter
1:jackson
2:steven
```

With existing

```shell
docker exec -it kafka-3 kafka-console-producer --broker-list kafka-2:29092  --topic last-name --property "parse.key=true" --property "key.separator=:"

1:banks
2:pears
2:sanders
2:holly
```

Results

```shell
peter banks
jackson banks
steven holly
```

As expected from documentation the GlobalKTable will load up all the data first before starting the application. If this
is the case then we will always join against the tables latest value.

#### Turning off cache tests

Back to the simple self join example but with cache turned off.

```shell
  streamsConfiguration[StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG] = 0
```

KTable self join.

```shell
  val nameKTable = streamsBuilder
            .table("name", Consumed.with(Serdes.String(), Serdes.String()))

  nameKTable
            .toStream()
            .peek { key, value ->
                logger.info("Processing {}, {}", key, value)
            }
            .leftJoin(nameKTable, ValueJoiner { value1, value2 ->
                logger.info("Joining the Stream Name {} to the KTable Name {}", value1, value2)
                value2
            }, Joined.with(Serdes.String(), Serdes.String(), Serdes.String()))
            .to("name-formatted", Produced.with(Serdes.String(), Serdes.String()))
```

We send onto the name topic these values before starting the app with remember this topic is compacted.

```shell
tom:perks
tom:matthews
tom:stevens
sharon:news
sharon:car
tom:party
```

As expected we now process all the values. The buffering and cache layer does not merge the records.

```shell
Processing tom, perks
Joining the Stream Name perks to the KTable Name perks
Processing tom, matthews
Joining the Stream Name matthews to the KTable Name matthews
Processing tom, stevens
Joining the Stream Name stevens to the KTable Name stevens
Processing sharon, news
Joining the Stream Name news to the KTable Name news
Processing sharon, car
Joining the Stream Name car to the KTable Name car
Processing tom, party
Joining the Stream Name party to the KTable Name party
```

Output to the topic all the values.

```shell
perks
matthews
stevens
news
car
party
```

Lets do this same example and turn the cache back on.

```shell
tom:perks
tom:matthews
tom:stevens
sharon:news
sharon:car
tom:party
```

Results in the data being merged which is what we expected so there is no guarantee of compacting the data, it depends
on the `streamsConfiguration[StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG]` and consider `COMMIT_INTERVAL_MS_CONFIG`.

```shell
Processing sharon, car
Joining the Stream Name car to the KTable Name car
Processing tom, party
Joining the Stream Name party to the KTable Name party
```

Now as per this [JIRA](https://issues.apache.org/jira/browse/KAFKA-4113) you can set the timestamps of messages to 0 and
this will ensure the KTable behaves like a GlobalKTable.

Now lets follow this advice and use the custom timestamp extractor lets put the same data onto the topic. This time we
expect even with no cache that the data will only join with the latest timestamp record.

The data will still stream in order but the join will only ever be with the latest.

Place the data on the topic done new data this time

```shell
clark:perks
clark:matthews
clark:stevens
sarah:news
sarah:car
clark:party
```

Interesting with the cache disabled and this custom timestamp extractor using zero we still process all events and join
with the same timstamp.

```shell
Processing sarah, news
Joining the Stream Name news to the KTable Name news
Processing sarah, car
Joining the Stream Name car to the KTable Name car
Processing clark, perks
Joining the Stream Name perks to the KTable Name perks
Processing clark, matthews
Joining the Stream Name matthews to the KTable Name matthews
Processing clark, stevens
Joining the Stream Name stevens to the KTable Name stevens
Processing clark, party
Joining the Stream Name party to the KTable Name party
```

If read further up you see why:

```shell
What you could do it, to write a custom timestamp extractor, and return `0` for each table side record and wall-clock time for each stream side record. In `extract()` to get a `ConsumerRecord` and can inspect the topic name to distinguish between both. Because `0` is smaller than wall-clock time, you can "bootstrap" the table to the end of the topic before any stream-side record gets processed.
```

We need to set zero only for the bootstrap but here we are doing a self join.

Therefore we can implement a custom transformer and change the timestamp to the correct one on the stream flow whilst
setting to zero on the KTable consume.

Here is the customer Timestamp extractor where all values are set to timestamp zero.

```shell
class IgnoreTimestampExtractor : TimestampExtractor {
    override fun extract(record: ConsumerRecord<Any, Any>?, partitionTime: Long): Long {
        // Ignore the timestamp and then start up.
        return 0
    }
}
```

Now we set this on the KTable to consume.

```shell
 val nameKTable = streamsBuilder
            .table(
                "name",
                Consumed.with(Serdes.String(), Serdes.String()).withTimestampExtractor(IgnoreTimestampExtractor())
            )
```

Now we setup the customer processor to change the stream timestamp.

```shell
 override fun transform(key: String?, value: String?): KeyValue<String, String>? {
        // In reality use the timestamp on the event
        context.forward(
            key,
            value,
            To.all().withTimestamp(LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli())
        )
        return null
    }
```

Now we place these messages onto the compacted topic.

```shell
clark:perks
clark:matthews
clark:stevens
sarah:news
sarah:car
clark:party
```

We start up the application and we can see it work correctly. We only join on the latest value and therefore we can
ensure if we compare timestamps we only use the latest value on the key when we join against the KTable. and could
filter older data in the stream.

```shell
Stream Processing Key sarah, Value news
Stream Processing Key sarah, Value car
Stream Processing Key clark, Value perks
Stream Processing Key clark, Value matthews
Stream Processing Key clark, Value stevens
Stream Processing Key clark, Value party
Joining the Stream Name news to the KTable Name car
Changed Timestamp Key sarah, Value car
Joining the Stream Name car to the KTable Name car
Changed Timestamp Key clark, Value perks
Joining the Stream Name perks to the KTable Name party
Changed Timestamp Key clark, Value matthews
Joining the Stream Name matthews to the KTable Name party
Changed Timestamp Key clark, Value stevens
Joining the Stream Name stevens to the KTable Name party
Changed Timestamp Key clark, Value party
Joining the Stream Name party to the KTable Name party
```

Now if we add back in the rekey example and run the data we get.

```shell
mark:sarah1
sarah:mark2
sarah:mark3
mark:sarah2
```

```shell
Stream Processing Key sarah, Value mark1
Changed Timestamp Key sarah, Value mark1
Stream Processing Key mark, Value sarah1
Changed Timestamp Key mark, Value sarah1
Stream Processing Key sarah, Value mark2
Changed Timestamp Key sarah, Value mark2
Stream Processing Key sarah, Value mark3
Changed Timestamp Key sarah, Value mark3
Stream Processing Key mark, Value sarah2
Changed Timestamp Key mark, Value sarah2
Joining the Stream Name mark1 to the KTable Name sarah2
Joining the Stream Name sarah1 to the KTable Name mark3
Joining the Stream Name mark2 to the KTable Name sarah2
Joining the Stream Name mark3 to the KTable Name sarah2
Joining the Stream Name sarah2 to the KTable Name mark3
```

## Tescontainers Integration Tests

Required Docker to be running.

`StreamIntegrationTest` uses [Testcontainers](https://www.testcontainers.org/) to fire up a running instance of Kafka
and Schema Registry and runs our application to drop messages on Kafka process them and read the output. Check it out a
very powerful example.

## Examples Understanding KStream Windowing

TODO a table of each event, event timestamp, the window its in and the  



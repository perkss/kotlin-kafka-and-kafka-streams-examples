# Kafka Reactive Producer Consumer Example

## Running the Example


Start up Kafka and Zookeeper:

`docker-compose up -d`

Create the required topic in Kafka:

```docker exec reactive-kafka-example-broker kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic lowercase-topic```

```docker exec reactive-kafka-example-broker kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic uppercase-topic```

Check the topic was created:

```docker exec reactive-kafka-example-broker kafka-topics --zookeeper zookeeper:2181 --list```

Produce some example data to the topic and with the Spring Boot Kafka Reactive App Running it will log the consumption of these messages:

```docker exec -it reactive-kafka-example-broker kafka-console-producer --broker-list broker:9092 --topic lowercase-topic --property "parse.key=true" --property "key.separator=:"```
              
For example

```1:perkss```                                                                                         

Check the topology output is uppercase with the console consumer

```docker exec reactive-kafka-example-broker kafka-console-consumer --bootstrap-server broker:9092 --topic uppercase-topic --property print.key=true --property key.separator="-" --from-beginning```


## Bad message fixes just like prod support
If you input a bad message onto a topic you can move the offset to the latest
`docker exec -it reactive-kafka-example-broker kafka-consumer-groups --bootstrap-server broker:9092 --group sample-group --reset-offsets --to-latest --topic lowercase-topic --execute`


## Shell into the Container

```sudo docker exec -i -t reactive-kafka-example-zookeeper /bin/bash```



```docker run \
      --net=host \
      --name=kafka-ssl-3 \
      -e KAFKA_ZOOKEEPER_CONNECT=localhost:2181 \
      -e KAFKA_ADVERTISED_LISTENERS=SSL://localhost:9092 \
      -e KAFKA_SSL_KEYSTORE_FILENAME=kafka.server.key.jks \
      -e KAFKA_SSL_KEYSTORE_CREDENTIALS=server_keystore_creds \
      -e KAFKA_SSL_KEY_CREDENTIALS=server_sslkey_creds \
      -e KAFKA_SSL_TRUSTSTORE_FILENAME=kafka.server.truststore.jks \
      -e KAFKA_SSL_TRUSTSTORE_CREDENTIALS=server_truststore_creds \
      -e KAFKA_SECURITY_INTER_BROKER_PROTOCOL=SSL \
      -v ${KAFKA_SSL_SECRETS_DIR}:/etc/kafka/secrets \
      confluentinc/cp-kafka:5.0.0```

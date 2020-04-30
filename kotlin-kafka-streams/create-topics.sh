#/bin/bash

docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --create --zookeeper localhost:22181 --replication-factor 3 --partitions 3 --topic order-request

docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --create --zookeeper localhost:22181 --replication-factor 3 --partitions 3 --topic order-processed
docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --create --zookeeper localhost:22181 --replication-factor 3 --partitions 3 --topic customer --config cleanup.policy=compact,delete
docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --create --zookeeper localhost:22181 --replication-factor 3 --partitions 3 --topic stock --config cleanup.policy=compact,delete

docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --zookeeper localhost:22181 --list

#docker run --rm  -it --net=host confluentinc/cp-schema-registry:latest kafka-avro-console-producer --broker-list localhost:9092 --topic order-request --property "parse.key=true" --property "key.separator=:" --property key.serializer=org.apache.kafka.common.serialization.StringSerializer --property value.schema='{ "namespace": "com.perkss.order.model", "type": "record", "name": "OrderRequested", "fields": [ { "name": "id", "type": { "type": "string", "avro.java.string": "String" } }, { "name": "product_id", "type": { "type": "string", "avro.java.string": "String" } } ] }'
#1:{"id": "1", "product_id": "abc"}
#http://0.0.0.0:8081/subjects/order-request-value/versions/1

# Populate the Stock Table
#docker run --rm  -it --net=host confluentinc/cp-schema-registry:latest kafka-avro-console-producer --broker-list localhost:9092 --topic customer --property "parse.key=true" --property "key.separator=:" --property key.serializer=org.apache.kafka.common.serialization.StringSerializer --property value.schema='{ "namespace": "com.perkss", "type": "record", "name": "Customer", "fields": [ { "name": "id", "type": { "type": "string", "avro.java.string": "String" } }, { "name": "name", "type": { "type": "string", "avro.java.string": "String" } }, { "name": "city", "type": { "type": "string", "avro.java.string": "String" } } ] }'
#1:{"id": "1", "name": "perkss", "city": "london"}
#http://0.0.0.0:8081/subjects/customer-value/versions/1

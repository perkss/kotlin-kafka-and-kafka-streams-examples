# Kotlin Kafka Reactive WebSockets
This module is an example of consuming off Kafka and processing them 
sending them on reactively to a websocket subscribing UI

## Building our DockerFile and running it

```shell script
cd kotlin-kafka-reactive-websockets
# Build the Jar
mvn clean install
# Build using Dockerfile in current directory
docker build --tag reactive-websocket:latest .
docker run --rm --network host --detach --name reactive-websocket reactive-websocket:latest
```

## Running The Example
```shell script
docker-compose up -d
docker exec -it kafka-1  kafka-console-producer --broker-list kafka-1:9092 --topic social-media-posts --property "parse.key=true" --property "key.separator=:"

```





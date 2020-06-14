# Kotlin Kafka Reactive WebSockets
This module is an example of consuming off Kafka and processing them 
sending them on reactively to a websocket subscribing UI

## Building our DockerFile and running it
```shell script
cd kotlin-kafka-reactive-web
# Build the Jar
mvn clean install
# Build using Dockerfile in current directory
docker build --tag reactive-web:latest .
# Run the create docker file and pass the container bootstrap servers
docker run --rm -p 8090:8090 -e perkss.kafka.example.bootstrap-servers=host.docker.internal:19092 --name reactive-web reactive-web:latest
```

## Running The Example
```shell script
docker-compose up -d
docker exec -it kafka-1  kafka-console-producer --broker-list kafka-1:9092 --topic social-media-posts --property "parse.key=true" --property "key.separator=:"
# Set up another consumer to listen to the messages to confirm they are coming
docker exec -it kafka-1 kafka-console-consumer --bootstrap-server kafka-1:9092 --topic social-media-posts
```





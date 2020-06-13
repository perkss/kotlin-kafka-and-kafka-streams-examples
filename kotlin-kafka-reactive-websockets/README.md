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
docker run -p 8090:8090 --detach --name reactive-websocket reactive-websocket:latest
```





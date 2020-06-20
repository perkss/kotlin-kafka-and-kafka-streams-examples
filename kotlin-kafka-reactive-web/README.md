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


## Running on Minishift

Start up docker and then Minishift.

```shell script
minishift start --iso-url centos
```

```
minishift docker-env
export DOCKER_TLS_VERIFY="1"
export DOCKER_HOST="tcp://192.168.99.101:2376"
export DOCKER_CERT_PATH="/Users/john/.minishift/certs"
export DOCKER_API_VERSION="1.24"
# Run this command to configure your shell:
# eval $(minishift docker-env)
```

```shell script
docker tag reactive-web $(minishift openshift registry)/myproject/reactive-web
```

```shell script
docker push $(minishift openshift registry)/myproject/reactive-web
```

```shell script
oc new-app --image-stream=reactive-web --name=reactive-web
oc expose service reactive-web
```

```shell script
http://reactive-web-myproject.192.168.64.8.nip.io/user/1
```

https://docs.okd.io/3.11/minishift/openshift/openshift-docker-registry.html#deploy-applications
https://docs.okd.io/3.11/minishift/using/docker-daemon.html






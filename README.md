# kotlin-kafka-examples
Kafka and Kafka Stream Examples in Kotlin with Project Reactor Kafka


## Docker Environment
Please use the docker-compose file in the root of the project to create the Kafka Brokers called `reactive-kafka-example-broker`
and a Zookeeper instance `reactive-kafka-example-zookeeper`

## Kafka Reactive Producer Consumer
Please check the directory README for details how to run this example.

This example shows how you can use the reactive API to build a consumer from a `lowercase-topic` map the data and output it
with the same key to a `uppercase-topic` with the data converted to uppercase. Please check the sub module README for 
how to execute this. Its a very interesting yet simple example, as you can see when the consume is lazily instantiated when 
it connects and then once a message is received it lazily instantiates the producer to send on.
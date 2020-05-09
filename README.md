# Reactive Kotlin Kafka and Kafka Streams with Kotlin
Kafka and Kafka Stream Examples in Kotlin with Project Reactor Kafka

## Docker Environment
Please use the docker-compose file in the root of each module project to create the Kafka Brokers and Zookeeper and where appropriate 
the Schema Registry.

Please check the directory README for details how to run this example.

## Integration Tests
Integration tests can be found for each module project and these require Docker to be running and use [Testcontainers](https://www.testcontainers.org/) these are powerful tests that fire up Kafka instances and our application and fully test the flow of messages through our streaming application.

## Kafka Reactive Producer Consumer
This example shows how you can use the reactive API to build a consumer from a `lowercase-topic` map the data and output it
with the same key to a `uppercase-topic` with the data converted to uppercase. Please check the sub module README for 
how to execute this. Its a very interesting yet simple example, as you can see when the consume is lazily instantiated when 
it connects and then once a message is received it lazily instantiates the producer to send on.

## Kafka Reactive Secure Producer Consumer
Shows how you can run a secured broker cluster using TLS and a application that will will consume and produce with this secure 
transport layer to the brokers. Details can be found in the sub folder README.

## Kafka Streams and Kotlin Examples
This module is for examples of using Kafka Streams with Kotlin and Avro. Here we build a stock ordering system that has the concept
 of customers to place orders. We use Avro to define schemas for the main topics and use changelog tables to store down product and 
 customer information which is joined to the OrderRequests. This module depends on the Avro code generation in the `avro-schemas` module
 so that needs building before compiling this module.

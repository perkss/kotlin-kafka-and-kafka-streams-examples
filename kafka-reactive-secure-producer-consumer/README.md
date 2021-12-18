## Kafka Reactive Secure Producer and Consumer Application

### Getting started via Script

Good you found your self the documentation for the secure cluster and producer and consumer application very important
stuff.

To create the required keys and truststores a handy script has been provided credit to confluent as based on their
script.

Change into the `secrets` directory and run `./create-certs` enter `yes`
a few times are you are ready.

Change back up a directory `../` and run the `docker-compose up -d` in detached mode or you can check the logs output.
Once this is up and running you can check the logs by running `docker-compose logs kafka-ssl-1` for example to check the
first broker logs.

#### Creating the topic

Bringing down a container locally and running on a localhost of the box.

This will no longer work as we need to have the zookeer_jaas.conf on the path.

```shell script
docker run --rm  --net=host confluentinc/cp-kafka:latest kafka-topics --create --zookeeper localhost:2181 --replication-factor 3 --partitions 3 --topic lowercase-topic
```

So we use the already running broker with the secrets to create the topic.

```shell script
ocker exec kafka1 kafka-topics --create --zookeeper zookeeper1:2181 --replication-factor 3 --partitions 3 --topic lowercase-topic
```

#### Starting the App

Start the Spring Boot App `SecureKafkaReactiveApp` as usual. (Note we will DockerFile this soon)

Or if you are running on the host you can also just use the existing kafka container running and it will have access to
the docker network like so

```shell script
docker exec kafka1 kafka-topics --create --zookeeper zookeeper1:2181 --replication-factor 3 --partitions 3 --topic lowercase-topic
```

#### Console Producing to the cluster securely

Accessing from the local host with the `--net=host` network

```shell script
docker run --net=host -v /Users/Stuart/Documents/Programming/kotlin/kotlin-kafka-examples/kafka-reactive-secure-producer-consumer/secrets:/etc/kafka/secrets -it confluentinc/cp-kafka:latest  kafka-console-producer --broker-list localhost:9094,localhost:9097,localhost:10000 --topic lowercase-topic --property "parse.key=true" --property "key.separator=:" --producer.config /etc/kafka/secrets/host.producer.ssl.config
```

Running from inside an existing container

```shell script
docker exec -it kafka1  kafka-console-producer --broker-list kafka1:29094,kafka2:29097,kafka3:30000 --topic lowercase-topic --property "parse.key=true" --property "key.separator=:" --producer.config /etc/kafka/secrets/host.producer.ssl.config
```

#### Console Consuming from the cluster securely

Accessing from the local host with the `--net=host` network

```shell script
docker run --net=host -v /Users/Stuart/Documents/Programming/kotlin/kotlin-kafka-examples/kafka-reactive-secure-producer-consumer/secrets:/etc/kafka/secrets -it confluentinc/cp-kafka:latest  kafka-console-consumer --bootstrap-server localhost:9093 --topic uppercase-topic --consumer.config /etc/kafka/secrets/host.consumer.ssl.config
```

Running from inside an existing container

```shell script
docker exec kafka1 kafka-console-consumer --bootstrap-server kafka1:29094 --topic uppercase-topic --property print.key=true --property key.separator="-" --from-beginning --consumer.config /etc/kafka/secrets/host.consumer.ssl.config
```

#### List consumers

```shell script
docker exec kafka1 kafka-consumer-groups  --list --bootstrap-server kafka1:29094 --command-config /etc/kafka/secrets/host.consumer.ssl.config
```

### TLS Security (Do It yourself)

Some pre reading to discuss difference between truststore (used to store public certificates) and keystore (used to
store private
certificates) [here](https://www.tutorialspoint.com/listtutorial/Difference-between-keystore-and-truststore-in-Java-SSL/4237)
and also
[confluents](https://docs.confluent.io/current/security/security_tutorial.html#generating-keys-certs) generating keys
and [authentication](https://docs.confluent.io/current/kafka/authentication_ssl.html). The keystore stores each
machine’s own identity. The truststore stores all the certificates that the machine should trust.

#### Generate Keystore

`keytool -keystore kafka.server.keystore.jks -alias localhost -validity 365 -genkey`

#### CA Authority and Creating Trust Store

Need a CA certificate which is a `.pem` file. Elastic provide good documentation for setting your
own [Certificate Authority](https://www.elastic.co/guide/en/shield/current/certificate-authority.html). You can use self
signed certificates such as exampled here by [Kafka](https://docs.confluent.io/2.0.0/kafka/ssl.html).

`openssl req -new -x509 -keyout ca-key -out ca-cert -days 365` creates the `ca-cert` and `ca-key`.

Lets now add the created certs to the trust store of the client so clients trust certificates signed by this CA. To
quote Kafka
"Importing a certificate into one’s truststore also means trusting all certificates that are signed by that
certificate."
"You can sign all certificates in the cluster with a single CA, and have all machines share the same truststore that
trusts the CA."

Server (inter broker communication) trust store import CA certificate so it trusts any certificate signed by CA.

`keytool -keystore kafka.server.truststore.jks -alias CARoot -import -file ca-cert`

Client (apps connecting too Kafka) trust store import CA certificate so it trusts any certificate signed by CA.

`keytool -keystore kafka.client.truststore.jks -alias CARoot -import -file ca-cert`

#### Signing the certificate

Export certificate from the Keystore:

`keytool -keystore kafka.server.keystore.jks -alias localhost -certreq -file cert-file`

Sign it with the generated CA created above.

`openssl x509 -req -CA ca-cert -CAkey ca-key -in cert-file -out cert-signed -days 365 -CAcreateserial -passin pass:password`

Now the private key is signed from the keystore we need to import it again and the CA certificate.

`keytool -keystore kafka.server.keystore.jks -alias CARoot -import -file ca-cert keytool -keystore kafka.server.keystore.jks -alias localhost -import -file cert-signed`

As noted by Kafka blog post

The definitions of the parameters are the following:

* keystore: the location of the keystore
* ca-cert: the certificate of the CA
* ca-key: the private key of the CA
* ca-password: the passphrase of the CA
* cert-file: the exported, unsigned certificate of the server
* cert-signed: the signed certificate of the server

### Running the secure cluster

You will need to generate the files for the broker communication and then can specify there location with the provided
environment variable at run time.

### Useful Links

* [Handy plain docker](https://docs.confluent.io/5.0.0/installation/docker/docs/installation/clustered-deployment-ssl.html)
 



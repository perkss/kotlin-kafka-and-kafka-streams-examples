package com.perkss.stuart.kafka.example

import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import reactor.kafka.sender.KafkaSender
import java.util.*
import reactor.kafka.sender.SenderOptions
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.HashMap
import reactor.kafka.sender.SenderRecord
import reactor.core.publisher.Flux
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.concurrent.CountDownLatch
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

@SpringBootApplication
class SampleProducer {

    private val dateFormat: SimpleDateFormat? = SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy")

    companion object {
        private val logger = LoggerFactory.getLogger(SampleProducer::class.java)
    }


    fun createProducer(bootstrapServers: String): KafkaSender<Int, String> {
        val producerProps = HashMap<String, Any>()
        producerProps[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
        producerProps[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = IntegerSerializer::class.java
        producerProps[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java

        val senderOptions = SenderOptions.create<Int, String>(producerProps)
                .maxInFlight(1024)

        return KafkaSender.create(senderOptions)
    }

    fun sendMessages(sender: KafkaSender<Int, String>, topic: String, count: Int, latch: CountDownLatch) {
        sender.send(Flux.range(1, count)
                .map { i -> SenderRecord.create(ProducerRecord(topic, i, "Message_$i"), i) })
                .doOnError { e -> logger.error("Send failed", e) }
                .subscribe { r ->
                    val metadata = r.recordMetadata()
                    logger.info("Message %d sent successfully, topic-partition=%s-%d offset=%d timestamp=%s\n",
                            r.correlationMetadata(),
                            metadata.topic(),
                            metadata.partition(),
                            metadata.offset(),
                            dateFormat!!.format(Date(metadata.timestamp())))
                    latch.countDown()
                }
    }

}

fun main(args: Array<String>) {

    // TODO make this run in background of spring app
    //  SpringApplication.run(SampleProducer::class.java, *args)

    val sampleProducer = SampleProducer()

    val brokerList = "localhost:9092"
    val topic = "reactive-topic"

    val producer = sampleProducer.createProducer(brokerList)

    val count = 20
    val latch = CountDownLatch(count)

    sampleProducer.sendMessages(producer, topic, 4, latch)

    latch.await(10, TimeUnit.SECONDS)

    producer.close()

}
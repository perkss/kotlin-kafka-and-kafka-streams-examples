package com.perkss.kafka.reactive

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.*
import org.apache.kafka.streams.kstream.Produced
import org.hamcrest.CoreMatchers
import org.hamcrest.MatcherAssert
import org.junit.jupiter.api.Test
import java.util.*

class GenericAvroIntegrationTest {

    @Test
    fun shouldRoundTripGenericAvroDataThroughKafka() {
        val schema = Schema.Parser().parse(
                "{\"namespace\": \"io.confluent.examples.streams.avro\",\n" +
                        " \"type\": \"record\",\n" +
                        " \"name\": \"WikiFeed\",\n" +
                        " \"fields\": [\n" +
                        "     {\"name\": \"user\", \"type\": \"string\"},\n" +
                        "     {\"name\": \"is_new\", \"type\": \"boolean\"},\n" +
                        "     {\"name\": \"content\", \"type\": [\"string\", \"null\"]}\n" +
                        " ]\n" +
                        "}\nx"
        )
        val schemaRegistryClient = MockSchemaRegistry.getClientForScope(SCHEMA_REGISTRY_SCOPE)
        schemaRegistryClient.register("inputTopic-value", schema)
        val record: GenericRecord = GenericData.Record(schema)
        record.put("user", "alice")
        record.put("is_new", true)
        record.put("content", "lorem ipsum")
        val inputValues = listOf<Any>(record)

        //
        // Step 1: Configure and start the processor topology.
        //
        val builder = StreamsBuilder()
        val streamsConfiguration = Properties()
        streamsConfiguration[StreamsConfig.APPLICATION_ID_CONFIG] = "generic-avro-integration-test"
        streamsConfiguration[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = "dummy config"
        streamsConfiguration[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.ByteArray().javaClass.name
        streamsConfiguration[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = GenericAvroSerde::class.java
        streamsConfiguration[AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG] = MOCK_SCHEMA_REGISTRY_URL

        // Write the input data as-is to the output topic.
        //
        // Normally, because a) we have already configured the correct default serdes for keys and
        // values and b) the types for keys and values are the same for both the input topic and the
        // output topic, we would only need to define:
        //
        //   builder.stream(inputTopic).to(outputTopic);
        //
        // However, in the code below we intentionally override the default serdes in `to()` to
        // demonstrate how you can construct and configure a generic Avro serde manually.
        val stringSerde = Serdes.String()
        val genericAvroSerde: Serde<GenericRecord> = GenericAvroSerde()
        // Note how we must manually call `configure()` on this serde to configure the schema registry.
        // This is different from the case of setting default serdes (see `streamsConfiguration`
        // above), which will be auto-configured based on the `StreamsConfiguration` instance.
        genericAvroSerde.configure(
                Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL),  /*isKey*/
                false)
        val stream = builder.stream<String, GenericRecord>(inputTopic)
        stream.to(outputTopic, Produced.with(stringSerde, genericAvroSerde))
        try {
            TopologyTestDriver(builder.build(), streamsConfiguration).use { topologyTestDriver ->
                //
                // Step 2: Setup input and output topics.
                //
                val kafkaAvroSerializer = KafkaAvroSerializer(schemaRegistryClient)
                val input: TestInputTopic<String, Any> = topologyTestDriver
                        .createInputTopic(inputTopic,
                                Serdes.String().serializer(),
                                kafkaAvroSerializer)
                val output: TestOutputTopic<String, Any> = topologyTestDriver
                        .createOutputTopic(outputTopic,
                                Serdes.String().deserializer(),
                                KafkaAvroDeserializer(schemaRegistryClient))

                //
                // Step 3: Produce some input data to the input topic.
                //
                input.pipeValueList(inputValues)

                //
                // Step 4: Verify the application's output data.
                //
                MatcherAssert.assertThat(output.readValuesToList(), CoreMatchers.equalTo(inputValues))
            }
        } finally {
            MockSchemaRegistry.dropScope(SCHEMA_REGISTRY_SCOPE)
        }
    }

    companion object {
        // A mocked schema registry for our serdes to use
        private val SCHEMA_REGISTRY_SCOPE = GenericAvroIntegrationTest::class.java.name
        private val MOCK_SCHEMA_REGISTRY_URL = "mock://$SCHEMA_REGISTRY_SCOPE"
        private const val inputTopic = "inputTopic"
        private const val outputTopic = "outputTopic"
    }
}
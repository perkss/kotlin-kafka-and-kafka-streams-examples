package com.perkss.kafka.reactive.processor

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.kstream.TransformerSupplier
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.Punctuator
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.StoreBuilder
import org.apache.kafka.streams.state.Stores
import org.slf4j.LoggerFactory
import java.time.Duration

class PopulateLastNamePunctuator(private val state: KeyValueStore<String, String>) : Punctuator {

    companion object {
        private val logger = LoggerFactory.getLogger(PopulateLastNamePunctuator::class.java)
    }

    fun init() {
        state.put("alice1", "Parker")
        state.put("jasmine2", "Jasmine")
        state.put("bill3", "Preston")
    }

    override fun punctuate(timestamp: Long) {
        logger.info("Punctuator called and populating Data")
        // For example you could query a DB for a refresh every hour
        state.put("alice1", "Parker 2")
        state.put("jasmine2", "Jasmine 2")
        state.put("bill3", "Preston 2")
    }

}

class JoinFirstNamesToLastNames : Transformer<String, String, KeyValue<String, String>> {

    companion object {
        private val logger = LoggerFactory.getLogger(JoinFirstNamesToLastNames::class.java)
    }

    private lateinit var state: KeyValueStore<String, String>
    private lateinit var context: ProcessorContext

    override fun init(processorContext: ProcessorContext) {
        context = processorContext
        state = context.getStateStore("inmemory-last-name")
        // Punctuator does not run immediately we init the data.
        val populateLastNamePunctuator = PopulateLastNamePunctuator(state)
        populateLastNamePunctuator.init()
        // For the test we advance the wall clock in the test to show the updated the values.
        context.schedule(Duration.ofMinutes(5), PunctuationType.WALL_CLOCK_TIME, populateLastNamePunctuator)
    }

    override fun transform(key: String, value: String): KeyValue<String, String> {
        val fullname = "$value ${state.get(key)}"
        logger.info("Joined Key {} with Result {}", key, fullname)
        return KeyValue.pair(key, fullname)
    }

    override fun close() {
        logger.info("Closing Processor")
    }

}

internal class LastNameTransformerSupplier : TransformerSupplier<String, String, KeyValue<String, String>> {

    override fun get(): Transformer<String, String, KeyValue<String, String>> {
        return JoinFirstNamesToLastNames()
    }

    override fun stores(): Set<StoreBuilder<*>> {
        val keyValueStoreBuilder = Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("inmemory-last-name"),
            Serdes.String(),
            Serdes.String()
        )
            .withLoggingDisabled()
        return setOf(keyValueStoreBuilder)
    }
}

object JoinInMemoryStore {

    private val logger = LoggerFactory.getLogger(JoinInMemoryStore::class.java)

    fun enrichFlowFromInMemoryStore(
        firstNamesTopic: String,
        fullNameTopic: String
    ): Topology {
        val builder = StreamsBuilder()

        // consume the post created
        val input = builder.stream(firstNamesTopic, Consumed.with(Serdes.String(), Serdes.String()))

        // Have the same key as prerequisite
        val joined = input.transform(
            LastNameTransformerSupplier()
        )

        // stream joined first and last names
        joined
            .peek { key, value -> logger.info("Sending on Key {} value {}", key, value) }
            .to(fullNameTopic, Produced.with(Serdes.String(), Serdes.String()))
        return builder.build()
    }
}
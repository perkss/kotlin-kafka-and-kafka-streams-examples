package com.perkss.kafka.reactive.processor

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.slf4j.LoggerFactory

class RekeyStream<T, V>(private val rekey: (V) -> T) : Transformer<T, V, KeyValue<T, V>> {

    companion object {
        private val logger = LoggerFactory.getLogger(RekeyStream::class.java)
    }

    private lateinit var context: ProcessorContext

    override fun init(context: ProcessorContext) {
        this.context = context
    }

    override fun transform(key: T, value: V): KeyValue<T, V> {
        val newKey = rekey(value)
        logger.info("Transforming Key from $key to $newKey")
        return KeyValue.pair(newKey, value)
    }

    override fun close() {}
}
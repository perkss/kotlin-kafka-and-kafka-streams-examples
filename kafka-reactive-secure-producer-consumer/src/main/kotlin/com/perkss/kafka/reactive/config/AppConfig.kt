package com.perkss.kafka.reactive.config

import com.perkss.kafka.reactive.KafkaReactiveConsumer
import com.perkss.kafka.reactive.KafkaReactiveProducer
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration


@EnableConfigurationProperties(ReactiveKafkaAppProperties::class)
@Configuration
class AppConfig {

    @Bean
    fun reactiveProducer(propertiesReactiveKafka: ReactiveKafkaAppProperties) =
            KafkaReactiveProducer(propertiesReactiveKafka.bootstrapServers, propertiesReactiveKafka.sslEnabled, propertiesReactiveKafka.saslEnabled)

    @Bean
    fun reactiveConsumer(propertiesReactiveKafka: ReactiveKafkaAppProperties) =
            KafkaReactiveConsumer(propertiesReactiveKafka.bootstrapServers,
                    propertiesReactiveKafka.inputTopic, propertiesReactiveKafka.sslEnabled, propertiesReactiveKafka.saslEnabled)

}
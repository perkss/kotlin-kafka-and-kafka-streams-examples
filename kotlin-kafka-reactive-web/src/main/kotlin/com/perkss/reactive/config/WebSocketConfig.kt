package com.perkss.reactive.config

import com.perkss.reactive.UserHandler
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.web.reactive.HandlerMapping
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.server.WebSocketService
import org.springframework.web.reactive.socket.server.support.HandshakeWebSocketService
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter
import org.springframework.web.reactive.socket.server.upgrade.ReactorNettyRequestUpgradeStrategy
import reactor.kafka.receiver.KafkaReceiver
import reactor.kafka.receiver.ReceiverOptions
import java.util.*

@EnableConfigurationProperties
@Configuration
class WebSocketConfig {

    @Profile("!test")
    @Bean
    fun kafkaReceiver(kafkaProperties: ReactiveKafkaAppProperties): KafkaReceiver<String, String> {
        val consumerProps = Properties()
        consumerProps[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = kafkaProperties.bootstrapServers
        consumerProps[ConsumerConfig.GROUP_ID_CONFIG] = kafkaProperties.consumerGroupId
        consumerProps[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        consumerProps[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        consumerProps[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
        val consumerOptions = ReceiverOptions.create<String, String>(consumerProps)
            .subscription(Collections.singleton(kafkaProperties.inputTopic))
        return KafkaReceiver.create(consumerOptions)
    }

    @Bean
    fun webSocketHandlerMapping(webSocketHandler: WebSocketHandler): HandlerMapping {
        val map = mapOf("/social-media-posts" to webSocketHandler)
        return SimpleUrlHandlerMapping(map, -1)
    }

    @Bean
    fun handlerAdapter() = WebSocketHandlerAdapter()

    @Bean
    fun webSocketService(): WebSocketService? {
        return HandshakeWebSocketService(ReactorNettyRequestUpgradeStrategy())
    }

    @Bean
    fun userHandler(reactiveKafkaAppProperties: ReactiveKafkaAppProperties): UserHandler =
        UserHandler(reactiveKafkaAppProperties)

}
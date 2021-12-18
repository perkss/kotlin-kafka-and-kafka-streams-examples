package com.perkss.reactive

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.WebSocketSession
import reactor.core.publisher.Mono
import reactor.kafka.receiver.KafkaReceiver
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

@Component
class SocialMediaPostsHandler(private val kafkaReceiver: KafkaReceiver<String, String>) : WebSocketHandler {

    companion object {
        private val logger = LoggerFactory.getLogger(SocialMediaPostsHandler::class.java)
    }

    private val connections = ConcurrentHashMap<String, WebSocketSession>()

    // https://docs.spring.io/spring/docs/current/spring-framework-reference/web-reactive.html#websocket-intro-architecture
    override fun handle(session: WebSocketSession): Mono<Void> {
        connections[session.id] = session

        val input = session.receive()
            .delayElements(Duration.ofSeconds(1))
            .then()

        val output = session.send(
            kafkaReceiver
                .receive()
                .doOnNext { logger.info("Sending back message {}", it.value()) }
                .map { session.textMessage(it.value()) })

        return Mono.zip(input, output)
            .then()
            .doFinally {
                logger.info("Removing session")
                connections.remove(session.id)
            }
    }
}
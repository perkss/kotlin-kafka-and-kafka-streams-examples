package com.perkss.reactive

import com.fasterxml.jackson.annotation.JsonAutoDetect
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.perkss.social.media.model.PostCreated
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.WebSocketSession
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration
import java.time.Instant
import java.util.*
import java.util.function.BiFunction

@Component
class SocialMediaPostsHandler : WebSocketHandler {

    companion object {
        private val logger = LoggerFactory.getLogger(SocialMediaPostsHandler::class.java)
    }

    private val json: ObjectMapper = ObjectMapper().apply {
        setVisibilityChecker(this.serializationConfig.defaultVisibilityChecker
                .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
                .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withCreatorVisibility(JsonAutoDetect.Visibility.NONE))
    }

    private val eventFlux: Flux<String> = Flux.generate { sink ->
        logger.info("Creating event")
        Mono.fromCallable {
            val event = PostCreated(
                    UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(),
                    "my fake posts",
                    Instant.now().toString())
            try {
                logger.info("Sinking event")
                sink.next(json.writeValueAsString(event))
            } catch (e: JsonProcessingException) {
                sink.error(e)
            }
        }.delayElement(Duration.ofSeconds(60L))
                .subscribe()
    }

    private val intervalFlux: Flux<String> = Flux.interval(Duration.ofMillis(10000L))
            .zipWith(eventFlux, BiFunction { _, event -> event })

    // https://docs.spring.io/spring/docs/current/spring-framework-reference/web-reactive.html#websocket-intro-architecture
    override fun handle(session: WebSocketSession): Mono<Void> {
        return session.send(session.receive()
                .doOnNext {
                    logger.info("Received Message")
                    it.retain()
                }
                .delayElements(Duration.ofSeconds(1)))
                .doOnNext { logger.info("Sending Message") }
//        return webSocketSession.send(eventFlux
//                .map(webSocketSession::textMessage))
//                .and(webSocketSession.receive()
//                        .map(WebSocketMessage::getPayloadAsText)
//                        .log())
    }


}
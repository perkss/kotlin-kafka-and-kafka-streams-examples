package com.perkss.reactive

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import org.springframework.web.reactive.socket.WebSocketHandler
import org.springframework.web.reactive.socket.WebSocketSession
import reactor.core.publisher.Mono
import java.time.Duration

// TODO investigate this
// https://github.com/joshlong/reactive-websockets-java-and-spring-integration/blob/master/src/main/java/com/example/integration/IntegrationApplication.java

@Component
class SocialMediaPostsHandler : WebSocketHandler {

    companion object {
        private val logger = LoggerFactory.getLogger(SocialMediaPostsHandler::class.java)
    }

    // https://docs.spring.io/spring/docs/current/spring-framework-reference/web-reactive.html#websocket-intro-architecture
    override fun handle(session: WebSocketSession): Mono<Void> {
        val publisher = session
                .receive()
                .doOnNext {
                    logger.info("Received Message on session ${session.id}")
                    it.retain()
                }
                .delayElements(Duration.ofSeconds(1))
                .map { session.textMessage("Processed ${it.payloadAsText}") }
                .doOnNext { logger.info("About to respond ${it.payloadAsText}") }

        return session.send(publisher)
    }


}
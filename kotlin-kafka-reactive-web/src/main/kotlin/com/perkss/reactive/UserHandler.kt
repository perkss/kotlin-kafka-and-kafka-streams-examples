package com.perkss.reactive

import com.perkss.reactive.config.ReactiveKafkaAppProperties
import kotlinx.coroutines.flow.flow
import org.json.JSONObject
import org.slf4j.LoggerFactory
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.server.*
import org.springframework.web.reactive.function.server.ServerResponse.ok
import java.util.concurrent.ConcurrentHashMap

private val store = ConcurrentHashMap<String, User>()

data class User(val id: String, val name: String)

class UserHandler(private val appProps: ReactiveKafkaAppProperties) {

    companion object {
        private val logger = LoggerFactory.getLogger(UserHandler::class.java)
    }

    suspend fun getUser(request: ServerRequest): ServerResponse {
        return ok()
                .contentType(MediaType.APPLICATION_JSON)
                .bodyAndAwait(flow { emit(JSONObject.wrap("{\"id\":\"${appProps.inputTopic}\"}")) })
    }

    suspend fun createUser(request: ServerRequest): ServerResponse {
        val user = request.awaitBody<User>()
        logger.info("Storing user {}", user.id)
        store[user.id] = user
        return ok().buildAndAwait()
    }


}
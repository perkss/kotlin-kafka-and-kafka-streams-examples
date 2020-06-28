package com.perkss.reactive.config

import com.perkss.reactive.UserHandler
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.MediaType.APPLICATION_JSON
import org.springframework.web.reactive.config.EnableWebFlux
import org.springframework.web.reactive.config.WebFluxConfigurer
import org.springframework.web.reactive.function.server.RouterFunction
import org.springframework.web.reactive.function.server.coRouter

@Configuration
@EnableWebFlux
class FunctionalRestConfig(val handler: UserHandler) : WebFluxConfigurer {

    @Bean
    fun routerFunctionA(): RouterFunction<*> {
        return coRouter {
            accept(APPLICATION_JSON).nest {
                GET("/user/{id}", handler::getUser)
                POST("/user", handler::createUser)
            }
        }
    }

}
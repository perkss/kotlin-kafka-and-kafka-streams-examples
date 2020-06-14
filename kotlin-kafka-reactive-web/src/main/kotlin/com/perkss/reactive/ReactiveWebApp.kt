package com.perkss.reactive

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class ReactiveWebApp

fun main(args: Array<String>) {
    runApplication<ReactiveWebApp>(*args)
}
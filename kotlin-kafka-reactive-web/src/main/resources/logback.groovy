import ch.qos.logback.classic.encoder.PatternLayoutEncoder

appender("CONSOLE", ConsoleAppender) {
    encoder(PatternLayoutEncoder) {
        pattern = "%date{\"yyyy-MM-dd'T'HH:mm:ss,SSSXXX\", UTC} [%thread] %-5level %class{36}.%M - %msg%n"
    }
}

root(INFO, ["CONSOLE"])
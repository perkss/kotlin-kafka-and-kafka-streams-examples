import ch.qos.logback.classic.encoder.PatternLayoutEncoder

appender("CONSOLE", ConsoleAppender) {
    encoder(PatternLayoutEncoder) {
        pattern = "%date{\"yyyy-MM-dd'T'HH:mm:ss,SSSXXX\", UTC} [%thread] - %msg%n"
    }
}
root(INFO, ["CONSOLE"])

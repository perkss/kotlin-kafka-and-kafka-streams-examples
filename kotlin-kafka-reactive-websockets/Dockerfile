FROM openjdk:11-jdk-slim

# COPY the build jar from the target folder
COPY target/kotlin-kafka-reactive-websockets-*T.jar reactive-websocket-example.jar
CMD ["java", "-jar", "/reactive-websocket-example.jar"]
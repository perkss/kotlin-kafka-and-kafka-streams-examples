FROM openjdk:11-jdk-slim
# COPY the build jar from the target folder
COPY target/kotlin-kafka-reactive-web-*T.jar reactive-web-example.jar
CMD ["java", "-jar", "/reactive-web-example.jar"]
EXPOSE 8080
EXPOSE 8778
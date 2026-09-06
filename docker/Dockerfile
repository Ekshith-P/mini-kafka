# ---- build stage: compile and package the fat jar ----
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
# Cache dependencies first so source-only changes don't re-download everything.
COPY pom.xml .
RUN mvn -q -DskipTests dependency:go-offline
COPY src ./src
RUN mvn -q -DskipTests package

# ---- run stage: small JRE image with just the jar and configs ----
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/target/mini-kafka.jar ./mini-kafka.jar
COPY config ./config
# Default: start broker 1. docker-compose overrides the command per broker.
ENTRYPOINT ["java", "-jar", "mini-kafka.jar"]
CMD ["broker", "config/docker/broker-1.properties"]
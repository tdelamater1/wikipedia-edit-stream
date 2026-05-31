# syntax=docker/dockerfile:1

# --- build stage: compile + package the fat jar ----------------------------
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
# Cache dependencies first (only re-downloads when pom.xml changes).
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN ./mvnw -B -q dependency:go-offline
COPY src/ src/
RUN ./mvnw -B -q clean package -DskipTests

# --- runtime stage: minimal JRE -------------------------------------------
FROM eclipse-temurin:21-jre
WORKDIR /app
# Run as an unprivileged user.
RUN useradd --system --uid 1001 --create-home appuser
COPY --from=build /build/target/ingest-*.jar /app/app.jar
USER 1001
EXPOSE 8080
# Let the JVM use most of the container memory limit.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0"
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

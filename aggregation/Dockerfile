# syntax=docker/dockerfile:1

# --- build stage: compile + package the fat jar ----------------------------
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN ./mvnw -B -q dependency:go-offline
COPY src/ src/
RUN ./mvnw -B -q clean package -DskipTests

# --- runtime stage: minimal JRE -------------------------------------------
FROM eclipse-temurin:21-jre
WORKDIR /app
RUN useradd --system --uid 1001 --create-home appuser
COPY --from=build /build/target/aggregation-*.jar /app/app.jar
USER 1001
EXPOSE 8080
# Leave headroom for RocksDB off-heap memory: don't let the heap claim the whole limit.
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=50.0"
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

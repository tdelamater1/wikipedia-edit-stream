package com.delamater.wikipedia.ingest.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.mongodb.autoconfigure.MongoClientSettingsBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.mongodb.connection.ClusterConnectionMode;

/**
 * Forces a direct connection (driver {@code directConnection=true}) when
 * {@code app.mongodb.direct-connection} is set. There is no Spring Boot property
 * for this, but it is essential for port-forwarded access: without it the driver
 * does replica-set topology discovery and tries to reach the member's in-cluster
 * DNS name ({@code mongodb-0.mongodb-svc...}), which is unreachable from a laptop.
 *
 * <p>{@link ClusterConnectionMode#SINGLE} is exactly what {@code directConnection=true}
 * sets under the hood. Set the property to {@code false} when running in-cluster,
 * where the member DNS resolves and normal replica-set discovery is preferable.
 */
@Configuration
public class MongoClientConfig {

    @Bean
    MongoClientSettingsBuilderCustomizer directConnectionCustomizer(
            @Value("${app.mongodb.direct-connection:true}") boolean directConnection) {
        return builder -> {
            if (directConnection) {
                builder.applyToClusterSettings(s -> s.mode(ClusterConnectionMode.SINGLE));
            }
        };
    }
}

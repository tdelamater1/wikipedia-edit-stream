package com.delamater.wikipedia.aggregation.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.mongodb.autoconfigure.MongoClientSettingsBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.mongodb.connection.ClusterConnectionMode;

/**
 * Forces a direct connection (driver {@code directConnection=true}) when
 * {@code app.mongodb.direct-connection} is set — required for laptop port-forwarded access,
 * where replica-set topology discovery would chase the member's unreachable in-cluster DNS.
 * Set the property to {@code false} in-cluster. (Boot 4 moved this interface to
 * {@code org.springframework.boot.mongodb.autoconfigure}.)
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

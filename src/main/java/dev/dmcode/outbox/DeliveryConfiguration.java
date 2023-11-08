package dev.dmcode.outbox;

import lombok.With;

import java.time.Duration;
import java.util.Objects;

@With
public record DeliveryConfiguration(
    int batchSize,
    Duration batchTimeout
) {
    private static final int DEFAULT_BATCH_SIZE = 10;
    private static final Duration DEFAULT_BATCH_TIMEOUT = Duration.ofSeconds(60);

    public DeliveryConfiguration {
        if (batchSize < 1) {
            throw new IllegalArgumentException("Batch size must be bigger than zero");
        }
        Objects.requireNonNull(batchTimeout, "Batch timeout must be provided");
        if (batchTimeout.isNegative()) {
            throw new IllegalArgumentException("Batch timeout must not be negative");
        }
    }

    public static DeliveryConfiguration createDefault() {
        return new DeliveryConfiguration(
            DEFAULT_BATCH_SIZE,
            DEFAULT_BATCH_TIMEOUT
        );
    }
}

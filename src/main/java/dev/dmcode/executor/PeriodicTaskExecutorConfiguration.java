package dev.dmcode.executor;

import lombok.With;

import java.time.Duration;
import java.util.Objects;

@With
public record PeriodicTaskExecutorConfiguration(
    Duration executionInterval,
    Duration onErrorPauseDuration,
    Duration threadTerminationTimeout
) {
    private static final Duration DEFAULT_EXECUTION_INTERVAL = Duration.ofSeconds(5);
    private static final Duration DEFAULT_ON_ERROR_PAUSE_DURATION = DEFAULT_EXECUTION_INTERVAL.dividedBy(2);
    private static final Duration DEFAULT_THREAD_TERMINATION_TIMEOUT = Duration.ofSeconds(60);

    public PeriodicTaskExecutorConfiguration {
        Objects.requireNonNull(executionInterval, "Execution interval must be provided");
        if (executionInterval.isNegative()) {
            throw new IllegalArgumentException("Execution interval must not be negative");
        }
        Objects.requireNonNull(onErrorPauseDuration, "On error pause duration must be provided");
        if (onErrorPauseDuration.isNegative()) {
            throw new IllegalArgumentException("On error pause duration must not be negative");
        }
        Objects.requireNonNull(threadTerminationTimeout, "Thread termination timeout must be provided");
        if (threadTerminationTimeout.isNegative()) {
            throw new IllegalArgumentException("Thread termination timeout must not be negative");
        }
    }

    public static PeriodicTaskExecutorConfiguration defaults() {
        return new PeriodicTaskExecutorConfiguration(
            DEFAULT_EXECUTION_INTERVAL,
            DEFAULT_ON_ERROR_PAUSE_DURATION,
            DEFAULT_THREAD_TERMINATION_TIMEOUT
        );
    }
}

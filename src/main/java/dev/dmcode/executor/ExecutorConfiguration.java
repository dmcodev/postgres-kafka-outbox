package dev.dmcode.executor;

import lombok.With;

import java.time.Duration;
import java.util.Objects;

@With
public record ExecutorConfiguration(
    Duration taskInterval,
    Duration onErrorPause,
    Duration threadTerminationTimeout
) {
    private static final Duration DEFAULT_TASK_INTERVAL = Duration.ofSeconds(5);
    private static final Duration DEFAULT_ON_ERROR_PAUSE = DEFAULT_TASK_INTERVAL.dividedBy(2);
    private static final Duration DEFAULT_THREAD_TERMINATION_TIMEOUT = Duration.ofSeconds(60);

    public ExecutorConfiguration {
        Objects.requireNonNull(taskInterval, "Task interval must be provided");
        if (taskInterval.isNegative()) {
            throw new IllegalArgumentException("Task interval must not be negative");
        }
        Objects.requireNonNull(onErrorPause, "On error pause must be provided");
        if (onErrorPause.isNegative()) {
            throw new IllegalArgumentException("On error pause must not be negative");
        }
        Objects.requireNonNull(threadTerminationTimeout, "Thread termination timeout must be provided");
        if (threadTerminationTimeout.isNegative()) {
            throw new IllegalArgumentException("Thread termination timeout must not be negative");
        }
    }

    public static ExecutorConfiguration createDefault() {
        return new ExecutorConfiguration(
            DEFAULT_TASK_INTERVAL,
            DEFAULT_ON_ERROR_PAUSE,
            DEFAULT_THREAD_TERMINATION_TIMEOUT
        );
    }
}

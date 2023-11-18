package dev.dmcode.executor

import spock.lang.Specification
import spock.lang.Unroll

import java.time.Duration

class PeriodicTaskExecutorConfigurationSpec extends Specification {

    @Unroll
    def "Should reject null values"() {
        when:
        PeriodicTaskExecutorConfiguration.defaults()
            .invokeMethod(METHOD, null)
        then:
        def thrown = thrown(NullPointerException)
        thrown.message == "$PROPERTY must be provided"
        where:
        METHOD                         | PROPERTY
        "withExecutionInterval"        | "Execution interval"
        "withOnErrorPauseDuration"     | "On error pause duration"
        "withThreadTerminationTimeout" | "Thread termination timeout"
    }

    @Unroll
    def "Should reject negative durations"() {
        when:
        PeriodicTaskExecutorConfiguration.defaults()
            .invokeMethod(METHOD, Duration.ofMillis(1).negated())
        then:
        def thrown = thrown(IllegalArgumentException)
        thrown.message == "$PROPERTY must not be negative"
        where:
        METHOD                         | PROPERTY
        "withExecutionInterval"        | "Execution interval"
        "withOnErrorPauseDuration"     | "On error pause duration"
        "withThreadTerminationTimeout" | "Thread termination timeout"
    }
}

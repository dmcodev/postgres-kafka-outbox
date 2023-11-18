package dev.dmcode.executor;

@FunctionalInterface
public interface PeriodicTask {

    PeriodicTaskResult runNext() throws Exception;

    default void onStop() throws Exception {}
}

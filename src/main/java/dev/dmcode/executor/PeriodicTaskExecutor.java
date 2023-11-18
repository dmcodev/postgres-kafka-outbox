package dev.dmcode.executor;

public interface PeriodicTaskExecutor {

    boolean start();

    boolean stop();

    void wakeup();
}

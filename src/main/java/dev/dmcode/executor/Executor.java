package dev.dmcode.executor;

public interface Executor {

    boolean start();

    boolean stop();

    void wakeup();
}

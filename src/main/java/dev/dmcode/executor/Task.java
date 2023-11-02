package dev.dmcode.executor;

@FunctionalInterface
public interface Task {

    TaskResult run() throws Exception;
}

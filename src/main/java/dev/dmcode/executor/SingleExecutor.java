package dev.dmcode.executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SingleExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleExecutor.class);

    private final Lock lock = new ReentrantLock();
    private final Condition wakeup = lock.newCondition();

    private final Task task;
    private final ExecutorConfiguration configuration;

    private Thread thread;
    private boolean wakeupRequested;

    public SingleExecutor(Task task, ExecutorConfiguration configuration) {
        this.task = Objects.requireNonNull(task, "Task must be provided");
        this.configuration = Objects.requireNonNull(configuration, "Executor configuration must be provided");
    }

    @Override
    public final boolean start() {
        boolean started = false;
        lock.lock();
        try {
            if (thread == null) {
                thread = new Thread(this::run);
                thread.start();
                started = true;
            }
        } finally {
            lock.unlock();
        }
        return started;
    }

    @Override
    public final boolean stop() {
        Thread stoppedThread = null;
        lock.lock();
        try {
            if (thread != null) {
                thread.interrupt();
                stoppedThread = thread;
                thread = null;
            }
        } finally {
            lock.unlock();
        }
        if (stoppedThread == null) {
            return false;
        }
        long joinMillis = configuration.threadTerminationTimeout().toMillis();
        if (joinMillis > 0) {
            try {
                stoppedThread.join(joinMillis);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                LOGGER.error("Interrupted while awaiting thread termination", exception);
            }
        }
        return !stoppedThread.isAlive();
    }

    @Override
    public final void wakeup() {
        lock.lock();
        try {
            if (thread != null) {
                wakeupRequested = true;
                wakeup.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    private void run() {
        while (notInterrupted()) {
            try {
                var taskResult = task.run();
                if (taskResult == TaskResult.AWAIT) {
                    await();
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            } catch (Exception exception) {
                LOGGER.error("Task execution exception", exception);
                pauseOnError();
            }
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void await() {
        long awaitMillis = configuration.taskInterval().toMillis();
        lock.lock();
        try {
            if (notInterrupted() && !wakeupRequested && awaitMillis > 0) {
                wakeup.await(awaitMillis, TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } finally {
            wakeupRequested = false;
            lock.unlock();
        }
    }

    private void pauseOnError() {
        long awaitMillis = configuration.onErrorPause().toMillis();
        if (notInterrupted() && awaitMillis > 0) {
            try {
                Thread.sleep(awaitMillis);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static boolean notInterrupted() {
        return !Thread.currentThread().isInterrupted();
    }
}

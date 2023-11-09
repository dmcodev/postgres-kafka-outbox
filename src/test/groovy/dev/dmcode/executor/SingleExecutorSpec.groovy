package dev.dmcode.executor

import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class SingleExecutorSpec extends Specification {

    def "Should start, run task and stop"() {
        given:
        def taskLatch = new CountDownLatch(1)
        def task = {
            taskLatch.countDown()
            TaskResult.AWAIT
        } as Task
        def configuration = ExecutorConfiguration.defaults()
        def executor = new SingleExecutor(task, configuration)
        when:
        boolean started = executor.start()
        taskLatch.await(5, TimeUnit.SECONDS)
        boolean stopped = executor.stop()
        then:
        started
        stopped
    }

    def "Should start only once"() {
        given:
        def task = { TaskResult.AWAIT } as Task
        def configuration = ExecutorConfiguration.defaults()
        def executor = new SingleExecutor(task, configuration)
        expect:
        executor.start()
        !executor.start()
        cleanup:
        executor.stop()
    }

    def "Should stop only once"() {
        given:
        def task = { TaskResult.AWAIT } as Task
        def configuration = ExecutorConfiguration.defaults()
        def executor = new SingleExecutor(task, configuration)
        expect:
        executor.start()
        !executor.start()
        cleanup:
        executor.stop()
    }

    def "Should await after task"() {
        given:
        def taskLatch = new CountDownLatch(2)
        def task = {
            taskLatch.countDown()
            TaskResult.AWAIT
        } as Task
        def configuration = ExecutorConfiguration.defaults()
        def executor = new SingleExecutor(task, configuration)
        when:
        executor.start()
        then:
        !taskLatch.await(250, TimeUnit.MILLISECONDS)
        cleanup:
        executor.stop()
    }

    def "Should wakeup after task"() {
        given:
        def taskStartedLatch = new CountDownLatch(1)
        def taskLatch = new CountDownLatch(2)
        def task = {
            taskStartedLatch.countDown()
            taskLatch.countDown()
            TaskResult.AWAIT
        } as Task
        def configuration = ExecutorConfiguration.defaults()
        def executor = new SingleExecutor(task, configuration)
        when:
        executor.start()
        taskStartedLatch.await(5, TimeUnit.SECONDS)
        Thread.sleep(250)
        executor.wakeup()
        then:
        taskLatch.await(5, TimeUnit.SECONDS)
        cleanup:
        executor.stop()
    }
}

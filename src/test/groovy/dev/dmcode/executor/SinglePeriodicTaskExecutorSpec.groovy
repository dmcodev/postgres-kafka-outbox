package dev.dmcode.executor

import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class SinglePeriodicTaskExecutorSpec extends Specification {

    def "Should start, run task and stop"() {
        given:
        def taskLatch = new CountDownLatch(1)
        def task = {
            taskLatch.countDown()
            PeriodicTaskResult.AWAIT
        } as PeriodicTask
        def configuration = PeriodicTaskExecutorConfiguration.defaults()
        def executor = new SinglePeriodicTaskExecutor(task, configuration)
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
        def task = { PeriodicTaskResult.AWAIT } as PeriodicTask
        def configuration = PeriodicTaskExecutorConfiguration.defaults()
        def executor = new SinglePeriodicTaskExecutor(task, configuration)
        expect:
        executor.start()
        !executor.start()
        cleanup:
        executor.stop()
    }

    def "Should stop only once"() {
        given:
        def task = { PeriodicTaskResult.AWAIT } as PeriodicTask
        def configuration = PeriodicTaskExecutorConfiguration.defaults()
        def executor = new SinglePeriodicTaskExecutor(task, configuration)
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
            PeriodicTaskResult.AWAIT
        } as PeriodicTask
        def configuration = PeriodicTaskExecutorConfiguration.defaults()
        def executor = new SinglePeriodicTaskExecutor(task, configuration)
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
            PeriodicTaskResult.AWAIT
        } as PeriodicTask
        def configuration = PeriodicTaskExecutorConfiguration.defaults()
        def executor = new SinglePeriodicTaskExecutor(task, configuration)
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

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
        def configuration = ExecutorConfiguration.createDefault()
        def executor = new SingleExecutor(task, configuration)
        when:
        boolean started = executor.start()
        taskLatch.await(5, TimeUnit.SECONDS)
        boolean stopped = executor.stop()
        then:
        started
        stopped
    }
}

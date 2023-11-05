package dev.dmcode.outbox

import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.spock.Testcontainers
import spock.lang.Shared
import spock.lang.Specification

import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.*

@Testcontainers
class StoreSpec extends Specification {

    @Shared
    PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:14.5-alpine")

    def "Should initialize schema once with multiple threads trying"() {
        given:
        int numberOfThreads = 16
        def executor = Executors.newFixedThreadPool(numberOfThreads)
        def startBarrier = new CyclicBarrier(numberOfThreads + 1)
        def completionLatch = new CountDownLatch(numberOfThreads)
        def exceptions = new CopyOnWriteArrayList()
        def storeConfiguration = StoreConfiguration.createDefault()
        def store = new Store(storeConfiguration)
        when:
        numberOfThreads.times {
            executor.submit {
                def connection = createJdbcConnection()
                startBarrier.await(5, TimeUnit.SECONDS)
                try {
                    store.initializeSchema(connection)
                } catch (Exception exception) {
                    exceptions.add(exception)
                }
                connection.close()
                completionLatch.countDown()
            }
        }
        startBarrier.await(5, TimeUnit.SECONDS)
        then:
        completionLatch.await(5, TimeUnit.SECONDS)
        exceptions.empty
        cleanup:
        try (def connection = createJdbcConnection()) {
            deleteSchema(connection, storeConfiguration)
        }
        executor.shutdownNow()
    }

    Connection createJdbcConnection() {
        DriverManager.getConnection(postgres.jdbcUrl, postgres.username, postgres.password)
    }

    void deleteSchema(Connection connection, StoreConfiguration configuration) {
        try (var statement = connection.createStatement()) {
            statement.execute("DROP TRIGGER \"${configuration.notifyTriggerName()}\" ON \"${configuration.schemaName()}\".\"${configuration.tableName()}\"")
            statement.execute("DROP FUNCTION \"${configuration.schemaName()}\".\"${configuration.notifyFunctionName()}\"")
            statement.execute("DROP TABLE \"${configuration.schemaName()}\".\"${configuration.tableName()}\"")
        }
    }
}

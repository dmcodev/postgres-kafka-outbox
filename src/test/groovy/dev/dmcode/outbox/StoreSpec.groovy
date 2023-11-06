package dev.dmcode.outbox

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.spock.Testcontainers
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

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
                try (def connection = createJdbcConnection()) {
                    startBarrier.await(5, TimeUnit.SECONDS)
                    store.initializeSchema(connection)
                } catch (Exception exception) {
                    exceptions.add(exception)
                }
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

    @Unroll
    def "Should insert and fetch records"() {
        given:
        def storeConfiguration = StoreConfiguration.createDefault()
        def store = new Store(storeConfiguration)
        def connection = createJdbcConnection()
        store.initializeSchema(connection)
        when:
        long recordId = store.insert(RECORD, RECORD.key(), RECORD.value(), connection)
        then:
        recordId == 1
        when:
        def records = store.selectForUpdate(10, connection)
        then:
        records.size() == 1
        with(records.first()) {
            id() == recordId
            with(record()) {
                topic() == RECORD.topic()
                partition() == RECORD.partition()
                timestamp() == RECORD.timestamp()
                key() == RECORD.key()
                value() == RECORD.value()
                headers() == RECORD.headers()
            }
        }
        cleanup:
        deleteSchema(connection, storeConfiguration)
        connection.close()
        where:
        RECORD << [
            new ProducerRecord<byte[], byte[]>("T", "V".bytes),
            new ProducerRecord<byte[], byte[]>("T", null),
            new ProducerRecord<byte[], byte[]>("T", "K".bytes, "V".bytes),
            new ProducerRecord<byte[], byte[]>("T", null, "V".bytes),
            new ProducerRecord<byte[], byte[]>("T", null, null, "K".bytes, "V".bytes),
            new ProducerRecord<byte[], byte[]>("T", 5, 100, "K".bytes, "V".bytes, [new RecordHeader("K", "V".bytes)])
        ]
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

package dev.dmcode.outbox

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.spock.Testcontainers
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.*

@Testcontainers
class StoreSpec extends Specification {

    @Shared
    PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:14.5-alpine")
    @Shared
    HikariDataSource dataSource

    def setup() {
        if (dataSource == null) {
            dataSource = createDataSource()
        }
    }

    def cleanupSpec() {
        dataSource?.close()
    }

    def "Should initialize schema once with multiple threads trying"() {
        given:
        int numberOfThreads = 16
        def executor = Executors.newFixedThreadPool(numberOfThreads)
        def startBarrier = new CyclicBarrier(numberOfThreads + 1)
        def completionLatch = new CountDownLatch(numberOfThreads)
        def exceptions = new CopyOnWriteArrayList()
        def storeConfiguration = StoreConfiguration.createDefault()
        def store = new Store(storeConfiguration, dataSource)
        when:
        numberOfThreads.times {
            executor.submit {
                startBarrier.await(5, TimeUnit.SECONDS)
                try {
                    store.initializeSchema()
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
        deleteSchema(storeConfiguration)
        executor.shutdownNow()
    }

    @Unroll
    def "Should insert and fetch records"() {
        given:
        def storeConfiguration = StoreConfiguration.createDefault()
        def store = new Store(storeConfiguration, dataSource)
        store.initializeSchema()
        def connection = dataSource.getConnection()
        when:
        long recordId = store.insert(RECORD, RECORD.key(), RECORD.value())
        then:
        recordId == 1
        when:
        def records = store.selectForUpdate(connection, 10)
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
        connection.close()
        deleteSchema(storeConfiguration)
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

    def "Should select for update"() {
        given:
        int numberOfRecords = 10
        int numberOfClients = 4
        int batchSize = 3
        def executor = Executors.newCachedThreadPool()
        def clientsBarrier = new CyclicBarrier(numberOfClients + 1)
        def fetchedBatches = new CopyOnWriteArrayList<List<Long>>()
        def record = new ProducerRecord<byte[], byte[]>("T", "V".bytes)
        def storeConfiguration = StoreConfiguration.createDefault()
        def store = new Store(storeConfiguration, dataSource)
        store.initializeSchema()
        numberOfRecords.times {
            store.insert(record, record.key(), record.value())
        }
        when:
        numberOfClients.times {
            executor.execute {
                try (def connection = dataSource.getConnection()) {
                    connection.setAutoCommit(false)
                    def batch = store.selectForUpdate(connection, batchSize).collect { it.id() }
                    fetchedBatches.add(batch)
                    clientsBarrier.await(10, TimeUnit.SECONDS)
                    connection.commit()
                } catch (Exception exception) {
                    exception.printStackTrace()
                }
            }
        }
        clientsBarrier.await(10, TimeUnit.SECONDS)
        then:
        fetchedBatches.size() == numberOfClients
        fetchedBatches.findAll { it.size() == batchSize }.size() == numberOfRecords.intdiv(batchSize)
        fetchedBatches.findAll { it.size() == (numberOfRecords % batchSize) }.size() == 1
        fetchedBatches.flatten().toSet() == (1 .. 10).toSet()
        cleanup:
        deleteSchema(storeConfiguration)
        executor.shutdownNow()
    }

    HikariDataSource createDataSource() {
        def config = new HikariConfig().tap {
            setJdbcUrl(postgres.jdbcUrl)
            setUsername(postgres.username)
            setPassword(postgres.password)
            setMaximumPoolSize(100)
        }
        new HikariDataSource(config)
    }

    void deleteSchema(StoreConfiguration configuration) {
        try (
            def connection = dataSource.getConnection()
            def statement = connection.createStatement()
        ) {
            statement.execute("DROP TRIGGER \"${configuration.notifyTriggerName()}\" ON \"${configuration.schemaName()}\".\"${configuration.tableName()}\"")
            statement.execute("DROP FUNCTION \"${configuration.schemaName()}\".\"${configuration.notifyFunctionName()}\"")
            statement.execute("DROP TABLE \"${configuration.schemaName()}\".\"${configuration.tableName()}\"")
        }
    }
}

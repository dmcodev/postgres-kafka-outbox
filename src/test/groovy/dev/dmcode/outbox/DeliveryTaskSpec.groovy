package dev.dmcode.outbox

import dev.dmcode.executor.PeriodicTaskResult
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeader
import spock.lang.Specification

import java.sql.Connection
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

class DeliveryTaskSpec extends Specification {

    private static final DELIVERY_CONFIGURATION = DeliveryConfiguration.defaults()
        .withBatchSize(3)
        .withBatchTimeout(Duration.ofMillis(500))

    def store = Mock(Store)
    def connection = Mock(Connection)
    def producer = Mock(Producer)

    def deliveryTask = new DeliveryTask(DELIVERY_CONFIGURATION, store, producer)

    def "Should deliver records batch"() {
        given:
        def producerRecord = new ProducerRecord<byte[], byte[]>("topic", 10, 100, "K".bytes, "V".bytes, [new RecordHeader("HK", "HV".bytes)])
        def records = (1 .. 3).collect { id -> new OutboxRecord(id, producerRecord) }
        when:
        def result = deliveryTask.runNext()
        then:
        result == PeriodicTaskResult.CONTINUE
        and:
        1 * store.getConnection() >> connection
        1 * connection.setAutoCommit(false)
        1 * store.selectForUpdate(connection, 3) >> records
        3 * producer.send(producerRecord, _) >> {
            def recordMetadata = buildRecordMetadata(it[0])
            (it[1] as Callback).onCompletion(recordMetadata, null)
            CompletableFuture.completedFuture(recordMetadata)
        }
        1 * store.delete(connection, [1L, 2L, 3L].toSet())
        1 * connection.commit()
        1 * connection.close()
        0 * _
    }

    def "Should delete only delivered records"() {
        given:
        def producerRecord = new ProducerRecord<byte[], byte[]>("topic", 10, 100, "K".bytes, "V".bytes, [new RecordHeader("HK", "HV".bytes)])
        def records = (1 .. 2).collect { id -> new OutboxRecord(id, producerRecord) }
        when:
        def result = deliveryTask.runNext()
        then:
        result == PeriodicTaskResult.AWAIT
        and:
        1 * store.getConnection() >> connection
        1 * connection.setAutoCommit(false)
        1 * store.selectForUpdate(connection, 3) >> records
        1 * producer.send(producerRecord, _) >> {
            def recordMetadata = buildRecordMetadata(it[0])
            (it[1] as Callback).onCompletion(recordMetadata, null)
            CompletableFuture.completedFuture(recordMetadata)
        }
        1 * producer.send(producerRecord, _) >> {
            def exception = new IOException()
            (it[1] as Callback).onCompletion(null, exception)
            CompletableFuture.failedFuture(exception)
        }
        1 * store.delete(connection, [1L].toSet())
        1 * connection.commit()
        1 * connection.close()
        0 * _
    }

    def "Should await after delivering non-full batch"() {
        given:
        def producerRecord = new ProducerRecord<byte[], byte[]>("topic", 10, 100, "K".bytes, "V".bytes, [new RecordHeader("HK", "HV".bytes)])
        def records = (1 .. 2).collect { id -> new OutboxRecord(id, producerRecord) }
        when:
        def result = deliveryTask.runNext()
        then:
        result == PeriodicTaskResult.AWAIT
        and:
        1 * store.getConnection() >> connection
        1 * connection.setAutoCommit(false)
        1 * store.selectForUpdate(connection, 3) >> records
        2 * producer.send(producerRecord, _) >> {
            def recordMetadata = buildRecordMetadata(it[0])
            (it[1] as Callback).onCompletion(recordMetadata, null)
            CompletableFuture.completedFuture(recordMetadata)
        }
        1 * store.delete(connection, [1L, 2L].toSet())
        1 * connection.commit()
        1 * connection.close()
        0 * _
    }

    def "Should delete only confirmed records on timeout"() {
        given:
        def producerRecord = new ProducerRecord<byte[], byte[]>("topic", 10, 100, "K".bytes, "V".bytes, [new RecordHeader("HK", "HV".bytes)])
        def records = (1 .. 2).collect { id -> new OutboxRecord(id, producerRecord) }
        def executor = Executors.newCachedThreadPool()
        def recordSequence = new AtomicInteger()
        when:
        def result = deliveryTask.runNext()
        then:
        result == PeriodicTaskResult.AWAIT
        and:
        1 * store.getConnection() >> connection
        1 * connection.setAutoCommit(false)
        1 * store.selectForUpdate(connection, 3) >> records
        2 * producer.send(producerRecord, _) >> {
            def recordMetadata = buildRecordMetadata(it[0])
            (it[1] as Callback).onCompletion(recordMetadata, null)
            def sequenceNumber = recordSequence.getAndIncrement()
            if (sequenceNumber == 0) {
                CompletableFuture.completedFuture(recordMetadata)
            } else if (sequenceNumber == 1) {
                CompletableFuture.supplyAsync({ Thread.sleep(1000) }, executor)
                    .thenApply { recordMetadata }
            }
        }
        1 * store.delete(connection, [1L].toSet())
        1 * connection.commit()
        1 * connection.close()
        0 * _
        cleanup:
        executor.shutdownNow()
    }

    private RecordMetadata buildRecordMetadata(ProducerRecord<?, ?> record) {
        def topicPartition = new TopicPartition(record.topic(), record.partition())
        new RecordMetadata(topicPartition, 0, 0, 0, 10, 10)
    }
}

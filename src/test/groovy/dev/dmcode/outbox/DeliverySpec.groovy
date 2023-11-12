package dev.dmcode.outbox

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import dev.dmcode.executor.ExecutorConfiguration
import dev.dmcode.executor.SingleExecutor
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.spock.Testcontainers
import org.testcontainers.utility.DockerImageName
import spock.lang.Shared
import spock.lang.Specification

import java.time.Duration

@Testcontainers
class DeliverySpec extends Specification {

    @Shared
    PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:14.5-alpine")
    @Shared
    KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
        .withEnv("KAFKA_NUM_PARTITIONS", "4")
    @Shared
    HikariDataSource dataSource

    def setupSpec() {
        dataSource = createDataSource()
    }

    def cleanupSpec() {
        dataSource.close()
    }

    def "Should deliver single record"() {
        given:
        def topicName = UUID.randomUUID().toString()
        def deliveryConfiguration = DeliveryConfiguration.defaults()
        def storeConfiguration = StoreConfiguration.defaults()
        def executorConfiguration = ExecutorConfiguration.defaults()
            .withTaskInterval(Duration.ofMillis(250))
        def store = new Store(storeConfiguration, dataSource)
        def kafkaProducer = createKafkaProducer()
        def deliveryTask = new DeliveryTask(deliveryConfiguration, store, kafkaProducer)
        def deliveryExecutor = new SingleExecutor(deliveryTask, executorConfiguration)
        def outboxProducer = new KafkaOutboxProducer(store, new StringSerializer(), new StringSerializer())
        def kafkaTopic = new KafkaTopic<String, String>(kafka.bootstrapServers, topicName, new StringDeserializer(), new StringDeserializer())
        and:
        store.initializeSchema()
        deliveryExecutor.start()
        when:
        outboxProducer.send(new ProducerRecord(topicName, "K", "V"))
        def deliveredRecords = kafkaTopic.poll(1)
        then:
        deliveredRecords["K"].value() == "V"
        cleanup:
        deliveryExecutor.stop()
        kafkaProducer.close()
        kafkaTopic.close()
    }

    def "Should deliver records with multiple delivery executors"() {
        given:
        def topicName = UUID.randomUUID().toString()
        def records = (1 .. 1000).collect { new ProducerRecord(topicName, "K_" + it, "V_" + it) }
        def deliveryConfiguration = DeliveryConfiguration.defaults()
            .withBatchSize(5)
        def storeConfiguration = StoreConfiguration.defaults()
        def executorConfiguration = ExecutorConfiguration.defaults()
            .withTaskInterval(Duration.ofMillis(250))
        def store = new Store(storeConfiguration, dataSource)
        def kafkaProducers = (1 .. 4).collect { createKafkaProducer() }
        def deliveryExecutors = (1 .. 4).collect {
            def deliveryTask = new DeliveryTask(deliveryConfiguration, store, kafkaProducers[it - 1])
            new SingleExecutor(deliveryTask, executorConfiguration)
        }
        def outboxProducer = new KafkaOutboxProducer(store, new StringSerializer(), new StringSerializer())
        def kafkaTopic = new KafkaTopic<String, String>(kafka.bootstrapServers, topicName, new StringDeserializer(), new StringDeserializer())
        and:
        store.initializeSchema()
        deliveryExecutors.forEach { it.start() }
        when:
        outboxProducer.send(records)
        def deliveredRecords = kafkaTopic.poll(1000)
        then:
        deliveredRecords.keySet() == (1 .. 1000).collect { "K_" + it }.toSet()
        cleanup:
        deliveryExecutors.forEach { it.stop() }
        kafkaProducers.forEach { it.close() }
        kafkaTopic.close()
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

    Producer<byte[], byte[]> createKafkaProducer() {
        new KafkaProducer(
            ["bootstrap.servers": kafka.bootstrapServers],
            new ByteArraySerializer(),
            new ByteArraySerializer()
        )
    }
}

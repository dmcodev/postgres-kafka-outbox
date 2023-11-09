package dev.dmcode.outbox

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.InterruptException
import org.apache.kafka.common.serialization.Deserializer

import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.TimeUnit
import java.util.stream.Collectors
import java.util.stream.Stream

class KafkaTopic<K, V> implements AutoCloseable {

    private final records = new LinkedBlockingDeque<ConsumerRecord<K, V>>()
    private final executor = Executors.newSingleThreadExecutor()
    private final Consumer<K, V> consumer
    private final String topicName

    KafkaTopic(String bootstrapServers, String topicName, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer) {
        def properties = [
            "bootstrap.servers": bootstrapServers,
            "group.id": UUID.randomUUID().toString(),
            "auto.offset.reset": "earliest"
        ]
        this.consumer = new KafkaConsumer(properties, keyDeserializer, valueDeserializer)
        this.topicName = topicName
        executor.execute { consume() }
    }

    Map<K, ConsumerRecord<K, V>> poll(int size) {
        Stream.generate { records.poll(10, TimeUnit.SECONDS) }
            .limit(size)
            .collect(Collectors.toMap({ it.key() }, { it }))
    }

    private void consume() {
        consumer.subscribe([topicName])
        while (!Thread.currentThread().isInterrupted()) {
            try {
                consumer.poll(Duration.ofMillis(1000))
                    .forEach(records::addLast)
            } catch (InterruptException ignored) {
                Thread.currentThread().interrupt()
            }
        }
    }

    @Override
    void close() throws Exception {
        executor.shutdownNow()
    }
}

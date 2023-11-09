package dev.dmcode.outbox;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Collection;
import java.util.List;

@RequiredArgsConstructor
public class KafkaOutboxProducer<K, V> {

    private final Store store;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    public long send(ProducerRecord<K, V> record) {
        var bytesRecord = createBytesRecord(record);
        return store.insert(List.of(bytesRecord)).get(0);
    }

    public List<Long> send(Collection<ProducerRecord<K, V>> records) {
        var bytesRecords = records.stream()
            .map(this::createBytesRecord)
            .toList();
        return store.insert(bytesRecords);
    }

    private ProducerRecord<byte[], byte[]> createBytesRecord(ProducerRecord<K, V> record) {
        return new ProducerRecord<>(
            record.topic(),
            record.partition(),
            record.timestamp(),
            keySerializer.serialize(record.topic(), record.headers(), record.key()),
            valueSerializer.serialize(record.topic(), record.headers(), record.value()),
            record.headers()
        );
    }
}

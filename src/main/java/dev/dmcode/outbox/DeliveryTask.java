package dev.dmcode.outbox;

import dev.dmcode.executor.Task;
import dev.dmcode.executor.TaskResult;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@RequiredArgsConstructor
class DeliveryTask implements Task {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeliveryTask.class);

    private final DeliveryConfiguration configuration;
    private final Store store;
    private final DataSource dataSource;
    private final Producer<byte[], byte[]> producer;

    @Override
    public TaskResult run() throws Exception {
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            var records = store.selectForUpdate(connection, configuration.batchSize());
            if (records.isEmpty()) {
                return TaskResult.AWAIT;
            }
            var deliveryResults = records.stream()
                .map(this::deliverRecord)
                .collect(Collectors.toList());
            var deliveredRecordIds = awaitRecordsDelivery(deliveryResults);
            if (!deliveredRecordIds.isEmpty()) {
                store.delete(connection, deliveredRecordIds);
                connection.commit();
            }
            return deliveredRecordIds.size() == configuration.batchSize() ? TaskResult.CONTINUE : TaskResult.AWAIT;
        }
    }

    private DeliveryResult deliverRecord(OutboxRecord record) {
        var deliveryFuture = producer.send(
            record.record(),
            (metadata, exception) -> onDeliveryResult(record.id(), metadata, exception)
        );
        return new DeliveryResult(record.id(), deliveryFuture);
    }

    private void onDeliveryResult(long recordId, RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            LOGGER.error("Could not deliver record with ID {}", recordId, exception);
        } else {
            LOGGER.debug("Record with ID {} delivered: {}", recordId, metadata);
        }
    }

    private Set<Long> awaitRecordsDelivery(List<DeliveryResult> deliveryResults) {
        long startTimestamp = System.currentTimeMillis();
        long elapsedMs;
        var deliveredRecordIds = new HashSet<Long>();
        for (var deliveryResult : deliveryResults) {
            elapsedMs = System.currentTimeMillis() - startTimestamp;
            long awaitMs = configuration.batchTimeout().isZero()
                ? Long.MAX_VALUE
                : configuration.batchTimeout().toMillis() - elapsedMs;
            if (awaitMs <= 0) {
                break;
            }
            if (awaitDeliveryResult(deliveryResult, awaitMs)) {
                deliveredRecordIds.add(deliveryResult.recordId());
            }
        }
        var notConfirmedRecordIds = deliveryResults.stream()
            .map(DeliveryResult::recordId)
            .filter(recordId -> !deliveredRecordIds.contains(recordId))
            .collect(Collectors.toSet());
        if (!notConfirmedRecordIds.isEmpty()) {
            LOGGER.warn("Could not confirm delivery of record IDs: {}", notConfirmedRecordIds);
        }
        return deliveredRecordIds;
    }

    private boolean awaitDeliveryResult(DeliveryResult deliveryResult, long awaitMs) {
        try {
            deliveryResult.future().get(awaitMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (TimeoutException timeoutException) {
            LOGGER.debug("Await timeout exceeded for record with ID {}: {} ms", deliveryResult.recordId(), awaitMs);
        } catch (Exception exception) {
            LOGGER.debug("Delivery await exception for record with ID {}", deliveryResult.recordId(), exception);
        }
        return false;
    }

    private record DeliveryResult(long recordId, Future<RecordMetadata> future) {}
}

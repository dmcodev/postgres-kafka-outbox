package dev.dmcode.outbox;

import org.apache.kafka.clients.producer.ProducerRecord;

record OutboxRecord(long id, ProducerRecord<byte[], byte[]> record) {}

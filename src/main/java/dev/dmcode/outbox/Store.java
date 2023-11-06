package dev.dmcode.outbox;

import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class Store {

    private static final String SCHEMA_SCRIPT_LOCATION = "/dev/dmcode/outbox/outbox_ddl.sql";
    private static final String INSERT_SQL_TEMPLATE = """
        INSERT INTO "%s"."%s" ("topic", "partition", "timestamp", "key", "value", "headers") VALUES (?, ?, ?, ?, ?, ?)
    """;
    private static final String SELECT_FOR_UPDATE_SQL_TEMPLATE = """
        SELECT "id", "topic", "partition", "timestamp", "key", "value", "headers" FROM "%s"."%s"
            ORDER BY "id" LIMIT %s FOR UPDATE SKIP LOCKED
    """;

    private final StoreConfiguration configuration;

    public Store() {
        this(StoreConfiguration.createDefault());
    }

    public Store(StoreConfiguration configuration) {
        this.configuration = configuration;
    }

    @SneakyThrows
    public void initializeSchema(Connection connection) {
        var placeholders = Map.of(
            "SCHEMA_NAME", configuration.schemaName(),
            "TABLE_NAME", configuration.tableName(),
            "NOTIFY_FUNCTION_NAME", configuration.notifyFunctionName(),
            "NOTIFY_TRIGGER_NAME", configuration.notifyTriggerName(),
            "NOTIFICATION_CHANNEL_NAME", configuration.notificationChannelName()
        );
        var schemaScript = loadSchemaScript(placeholders);
        try (var statement = connection.createStatement()) {
            statement.execute(schemaScript);
        }
    }

    @SneakyThrows
    long insert(ProducerRecord<?, ?> record, byte[] key, byte[] value, Connection connection) {
        var insertSql = INSERT_SQL_TEMPLATE.formatted(configuration.schemaName(), configuration.tableName());
        try (var statement = connection.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS)) {
            statement.setString(1, record.topic());
            statement.setObject(2, record.partition());
            statement.setObject(3, record.timestamp());
            statement.setBytes(4, key);
            statement.setBytes(5, value);
            statement.setBytes(6, HeadersCodec.serialize(record.headers()));
            if (statement.executeUpdate() != 1) {
                throw new IllegalStateException("Could not insert record");
            }
            try (var generatedKeys = statement.getGeneratedKeys()) {
                if (!generatedKeys.next()) {
                    throw new IllegalStateException("Could not retrieve inserted record key");
                }
                return generatedKeys.getLong(1);
            }
        }
    }

    @SneakyThrows
    List<OutboxRecord> selectForUpdate(int limit, Connection connection) {
        var selectSql = SELECT_FOR_UPDATE_SQL_TEMPLATE.formatted(configuration.schemaName(), configuration.tableName(), limit);
        try (
            var statement = connection.prepareStatement(selectSql, Statement.RETURN_GENERATED_KEYS);
            var resultSet = statement.executeQuery()
        ) {
            var records = new ArrayList<OutboxRecord>(limit);
            while (resultSet.next()) {
                records.add(deserialize(resultSet));
            }
            return records;
        }
    }

    private static OutboxRecord deserialize(ResultSet resultSet) throws SQLException {
        long id = resultSet.getLong(1);
        String topic = resultSet.getString(2);
        Integer partition = resultSet.getObject(3, Integer.class);
        Long timestamp = resultSet.getObject(4, Long.class);
        var key = resultSet.getBytes(5);
        var value = resultSet.getBytes(6);
        var headersBytes = resultSet.getBytes(7);
        var record = new ProducerRecord<>(topic, partition, timestamp, key, value);
        var headers = HeadersCodec.deserialize(headersBytes);
        if (headers != null) {
            for (var header : headers) {
                record.headers().add(header);
            }
        }
        return new OutboxRecord(id, record);
    }

    private String loadSchemaScript(Map<String, String> placeholders) throws IOException  {
        var inputStream = Optional.ofNullable(getClass().getResourceAsStream(SCHEMA_SCRIPT_LOCATION))
            .orElseThrow(() -> new IllegalStateException("Could not load SQL script from the classpath location: " + SCHEMA_SCRIPT_LOCATION));
        try (inputStream) {
            var script = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            for (var placeholder : placeholders.entrySet()) {
                script = script.replace("{" + placeholder.getKey() + "}", placeholder.getValue());
            }
            return script;
        }
    }
}

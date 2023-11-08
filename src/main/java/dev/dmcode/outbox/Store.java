package dev.dmcode.outbox;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class Store {

    private static final String SCHEMA_SCRIPT_LOCATION = "/dev/dmcode/outbox/outbox_ddl.sql";
    private static final String INSERT_PLACEHOLDERS_CLAUSE = "(?, ?, ?, ?, ?, ?)";
    private static final String INSERT_SQL_TEMPLATE = """
        INSERT INTO "%s"."%s" ("topic", "partition", "timestamp", "key", "value", "headers") VALUES %s
    """;
    private static final String SELECT_FOR_UPDATE_SQL_TEMPLATE = """
        SELECT "id", "topic", "partition", "timestamp", "key", "value", "headers" FROM "%s"."%s"
            ORDER BY "id" LIMIT %s FOR UPDATE SKIP LOCKED
    """;
    private static final int INSERT_BATCH_LIMIT = 100;
    private static final int DELETE_BATCH_LIMIT = 1000;
    private static final String DELETE_SQL_TEMPLATE = """
        DELETE FROM "%s"."%s" WHERE "id" IN (%s)
    """;

    private final StoreConfiguration configuration;
    private final DataSource dataSource;

    public Store(DataSource dataSource) {
        this(StoreConfiguration.createDefault(), dataSource);
    }

    @SneakyThrows
    public void initializeSchema() {
        var placeholders = Map.of(
            "SCHEMA_NAME", configuration.schemaName(),
            "TABLE_NAME", configuration.tableName(),
            "NOTIFY_FUNCTION_NAME", configuration.notifyFunctionName(),
            "NOTIFY_TRIGGER_NAME", configuration.notifyTriggerName(),
            "NOTIFICATION_CHANNEL_NAME", configuration.notificationChannelName()
        );
        var schemaScript = loadSchemaScript(placeholders);
        try (
            var connection = dataSource.getConnection();
            var statement = connection.createStatement()
        ) {
            statement.execute(schemaScript);
        }
    }

    @SneakyThrows
    List<Long> insert(List<ProducerRecord<byte[], byte[]>> records) {
        int insertedRecords = 0;
        var keys = new ArrayList<Long>(records.size());
        try (var connection = dataSource.getConnection()) {
            PreparedStatement batchStatement = null;
            while (insertedRecords < records.size()) {
                int remainingToInsert = records.size() - insertedRecords;
                if (remainingToInsert >= INSERT_BATCH_LIMIT) {
                    if (batchStatement == null) {
                        batchStatement = createInsertStatement(connection, INSERT_BATCH_LIMIT);
                    }
                    batchStatement.clearParameters();
                    insertedRecords = insertBatch(records, INSERT_BATCH_LIMIT, insertedRecords, batchStatement, keys);
                } else {
                    try (var remainingStatement = createInsertStatement(connection, remainingToInsert)) {
                        insertedRecords = insertBatch(records, remainingToInsert, insertedRecords, remainingStatement, keys);
                    }
                }
            }
            if (batchStatement != null) {
                batchStatement.close();
            }
            if (keys.size() != records.size()) {
                throw new IllegalStateException("Could not insert all requested records: " + records);
            }
            return keys;
        }
    }

    @SneakyThrows
    List<OutboxRecord> selectForUpdate(Connection connection, int limit) {
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

    @SneakyThrows
    void delete(Connection connection, Set<Long> keys) {
        var pendingKeys = new HashSet<Object>(keys);
        while (!pendingKeys.isEmpty()) {
            var keysBatch = pendingKeys.stream().limit(DELETE_BATCH_LIMIT).collect(Collectors.toSet());
            pendingKeys.removeAll(keysBatch);
            var sqlKeySet = keysBatch.stream().map(Object::toString).collect(Collectors.joining(", "));
            var deleteSql = DELETE_SQL_TEMPLATE.formatted(configuration.schemaName(), configuration.tableName(), sqlKeySet);
            try (var statement = connection.prepareStatement(deleteSql)) {
                if (statement.executeUpdate() != keysBatch.size()) {
                    throw new IllegalStateException("Could not delete all requested records: " + keysBatch);
                }
            }
        }
    }

    private static int insertBatch(
        List<ProducerRecord<byte[], byte[]>> records,
        int batchSize,
        int insertedRecords,
        PreparedStatement statement,
        ArrayList<Long> keys
    ) throws SQLException {
        int column = 1;
        for (int i = 0; i < batchSize; i++) {
            var record = records.get(insertedRecords++);
            statement.setString(column++, record.topic());
            statement.setObject(column++, record.partition());
            statement.setObject(column++, record.timestamp());
            statement.setBytes(column++, record.key());
            statement.setBytes(column++, record.value());
            statement.setBytes(column++, HeadersCodec.serialize(record.headers()));
        }
        statement.execute();
        try (var resultSet = statement.getGeneratedKeys()) {
            while (resultSet.next()) {
                keys.add(resultSet.getLong(1));
            }
        }
        return insertedRecords;
    }

    @SuppressWarnings("SqlSourceToSinkFlow")
    private PreparedStatement createInsertStatement(Connection connection, int batchSize) throws SQLException{
        var placeholdersClauseSql = Stream.generate(() -> INSERT_PLACEHOLDERS_CLAUSE).limit(batchSize).collect(Collectors.joining(", "));
        var insertSql = INSERT_SQL_TEMPLATE.formatted(configuration.schemaName(), configuration.tableName(), placeholdersClauseSql);
        return connection.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS);
    }

    private static OutboxRecord deserialize(ResultSet resultSet) throws SQLException {
        int column = 1;
        long id = resultSet.getLong(column++);
        var topic = resultSet.getString(column++);
        var partition = resultSet.getObject(column++, Integer.class);
        var timestamp = resultSet.getObject(column++, Long.class);
        var key = resultSet.getBytes(column++);
        var value = resultSet.getBytes(column++);
        var headersBytes = resultSet.getBytes(column);
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

package dev.dmcode.outbox;

import lombok.SneakyThrows;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;

public class Store {

    private static final String SCHEMA_SCRIPT_LOCATION = "/dev/dmcode/outbox/outbox_ddl.sql";

    private final StoreConfiguration configuration;

    public Store() {
        this(StoreConfiguration.createDefault());
    }

    public Store(StoreConfiguration configuration) {
        this.configuration = configuration;
    }

    public void initializeSchema(Connection connection) throws SQLException {
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
    private String loadSchemaScript(Map<String, String> placeholders) {
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

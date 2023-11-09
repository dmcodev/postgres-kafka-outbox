package dev.dmcode.outbox;

import lombok.With;

import java.util.Objects;

@With
public record StoreConfiguration(
    String schemaName,
    String tableName,
    String notifyFunctionName,
    String notifyTriggerName,
    String notificationChannelName
) {
    private static final String DEFAULT_SCHEMA_NAME = "public";
    private static final String DEFAULT_TABLE_NAME = "kafka_outbox";
    private static final String DEFAULT_NOTIFY_FUNCTION_NAME = "kafka_outbox_notify";
    private static final String DEFAULT_NOTIFY_TRIGGER_NAME = "kafka_outbox_notify_trigger";
    private static final String DEFAULT_NOTIFICATION_CHANNEL_NAME = "kafka_outbox_notifications";

    public StoreConfiguration {
        Objects.requireNonNull(schemaName, "Schema name must be provided");
        Objects.requireNonNull(tableName, "Table name must be provided");
        Objects.requireNonNull(notifyFunctionName, "Notify function name must be provided");
        Objects.requireNonNull(notifyTriggerName, "Notify trigger name must be provided");
        Objects.requireNonNull(notificationChannelName, "Notification channel name must be provided");
    }

    String qualifiedNotificationChannelName() {
        return String.format("%s_%s", schemaName(), notificationChannelName());
    }

    public static StoreConfiguration defaults() {
        return new StoreConfiguration(
            DEFAULT_SCHEMA_NAME,
            DEFAULT_TABLE_NAME,
            DEFAULT_NOTIFY_FUNCTION_NAME,
            DEFAULT_NOTIFY_TRIGGER_NAME,
            DEFAULT_NOTIFICATION_CHANNEL_NAME
        );
    }
}

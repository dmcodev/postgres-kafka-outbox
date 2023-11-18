package dev.dmcode.outbox;

import dev.dmcode.executor.PeriodicTask;
import dev.dmcode.executor.PeriodicTaskExecutor;
import dev.dmcode.executor.PeriodicTaskResult;
import lombok.RequiredArgsConstructor;
import org.postgresql.jdbc.PgConnection;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Duration;

@RequiredArgsConstructor
class NotificationTask implements PeriodicTask {

    private final DataSource dataSource;
    private final PeriodicTaskExecutor deliveryExecutor;
    private final String channelName;
    private final Duration pollTimeout;

    private PgConnection connection;

    @Override
    public PeriodicTaskResult runNext() throws SQLException {
        try {
            initializeConnection();
            pollNotifications();
        } catch (SQLException exception) {
            closeConnection();
            throw exception;
        }
        return PeriodicTaskResult.CONTINUE;
    }

    @Override
    public void onStop() throws SQLException {
        closeConnection();
    }

    private void initializeConnection() throws SQLException {
        if (connection != null) {
            return;
        }
        connection = dataSource.getConnection()
            .unwrap(PgConnection.class);
        try (var statement = connection.createStatement()) {
            statement.executeUpdate("LISTEN \"" + channelName + "\"");
        }
    }

    private void pollNotifications() throws SQLException {
        var notifications = connection.getNotifications((int) pollTimeout.toMillis());
        if (notifications.length > 0) {
            deliveryExecutor.wakeup();
        }
    }

    private void closeConnection() throws SQLException {
        if (connection == null) {
            return;
        }
        try (var statement = connection.createStatement()) {
            statement.executeUpdate("UNLISTEN \"" + channelName + "\"");
        }
        connection.close();
        connection = null;
    }
}

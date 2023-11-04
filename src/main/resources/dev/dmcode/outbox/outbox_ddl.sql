SELECT pg_advisory_xact_lock(3703611250150143094);
DO $outbox$
    BEGIN
        IF ((SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{SCHEMA_NAME}' AND table_name = '{TABLE_NAME}') = 0)
        THEN
            CREATE TABLE "{SCHEMA_NAME}"."{TABLE_NAME}"(
                "id" BIGSERIAL PRIMARY KEY,
                "topic" VARCHAR(255) NOT NULL,
                "partition" INTEGER,
                "timestamp" BIGINT,
                "key" BYTEA,
                "value" BYTEA NOT NULL,
                "headers" BYTEA
            );
            CREATE FUNCTION "{SCHEMA_NAME}"."{NOTIFY_FUNCTION_NAME}"() RETURNS TRIGGER AS $function$
                DECLARE
                BEGIN
                    NOTIFY "{SCHEMA_NAME}_{NOTIFICATION_CHANNEL_NAME}";
                    RETURN NEW;
                END;
            $function$ LANGUAGE plpgsql;
            CREATE TRIGGER "{NOTIFY_TRIGGER_NAME}" AFTER INSERT ON "{SCHEMA_NAME}"."{TABLE_NAME}"
                FOR EACH STATEMENT
                EXECUTE PROCEDURE "{SCHEMA_NAME}"."{NOTIFY_FUNCTION_NAME}"();
        END IF;
    END;
$outbox$ LANGUAGE plpgsql;
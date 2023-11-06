package dev.dmcode.outbox;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class HeadersCodec {

    private static final byte PROTOCOL_VERSION = 1;

    @SneakyThrows
    static byte[] serialize(Headers headers) {
        if (headers == null) {
            return null;
        }
        int estimatedSerializedSize = estimateSerializedSize(headers);
        var outputBuffer = new ByteArrayOutputStream(estimatedSerializedSize);
        try (var outputStream = new DataOutputStream(outputBuffer)) {
            outputStream.writeByte(PROTOCOL_VERSION);
            for (var header : headers) {
                serialize(header, outputStream);
            }
        }
        return outputBuffer.toByteArray();
    }

    @SneakyThrows
    static Headers deserialize(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        var headers = new RecordHeaders();
        try (var inputStream = new DataInputStream(new ByteArrayInputStream(bytes))) {
            byte protocolVersion = inputStream.readByte();
            if (protocolVersion != PROTOCOL_VERSION) {
                throw new IllegalStateException("Protocol version not supported: " + protocolVersion);
            }
            while (inputStream.available() > 0) {
                headers.add(deserialize(inputStream));
            }
        }
        return headers;
    }

    private static void serialize(Header header, DataOutputStream output) throws IOException {
        serializeKey(header.key(), output);
        serializeValue(header.value(), output);
    }

    private static void serializeKey(String key, DataOutputStream output) throws IOException {
        var bytes = key.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static void serializeValue(byte[] value, DataOutputStream output) throws IOException {
        if (value != null) {
            output.writeInt(value.length);
            output.write(value);
        } else {
            output.writeInt(-1);
        }
    }

    private static Header deserialize(DataInputStream input) throws IOException {
        return new RecordHeader(deserializeKey(input), deserializeValue(input));
    }

    private static String deserializeKey(DataInputStream input) throws IOException {
        byte[] bytes = new byte[input.readInt()];
        input.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static byte[] deserializeValue(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length >= 0) {
            byte[] bytes = new byte[length];
            input.readFully(bytes);
            return bytes;
        }
        return null;
    }

    static int estimateSerializedSize(Headers headers) {
        int size = 1;
        for (var header : headers) {
            size += estimateSerializedSize(header);
        }
        return size;
    }

    private static int estimateSerializedSize(Header header) {
        int size = 8;
        if (header.key() != null) {
            size += header.key().length();
        }
        if (header.value() != null) {
            size += header.value().length;
        }
        return size;
    }
}

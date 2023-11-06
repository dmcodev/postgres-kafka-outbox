package dev.dmcode.outbox

import org.apache.kafka.common.header.internals.RecordHeaders
import spock.lang.Specification
import spock.lang.Unroll

class HeadersCodecSpec extends Specification {

    @Unroll
    def "Should serialize and deserialize record headers"() {
        given:
        def serialized= HeadersCodec.serialize(HEADERS)
        expect:
        HeadersCodec.deserialize(serialized) == HEADERS
        where:
        HEADERS << [
            null,
            new RecordHeaders(),
            new RecordHeaders().add("K", null),
            new RecordHeaders().add("", new byte[] {}),
            new RecordHeaders().add("KA", "A".bytes).add("KB", "B".bytes),
            new RecordHeaders().add("Ą", "Ś".bytes)
        ]
    }

    @Unroll
    def "Should correctly estimate serialized size"() {
        given:
        def serialized= HeadersCodec.serialize(HEADERS)
        expect:
        HeadersCodec.estimateSerializedSize(HEADERS) == serialized.length
        where:
        HEADERS << [
            new RecordHeaders(),
            new RecordHeaders().add("K", null),
            new RecordHeaders().add("", new byte[] {}),
            new RecordHeaders().add("KA", "A".bytes).add("KB", "B".bytes)
        ]
    }
}

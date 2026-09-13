package com.minikafka.protocol;

import com.minikafka.protocol.messages.ProduceRequest;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class SerializationTest {

    @Test
    void primitivesRoundTrip() {
        ByteWriter w = new ByteWriter();
        w.putShort(0x1234);
        w.putInt(0x0A0B0C0D);
        w.putLong(1234567890123L);
        w.putString("hello");
        w.putString(null);
        w.putBytes(new byte[]{1, 2, 3});
        w.putBytes(null);
        w.putIntArray(new int[]{5, 6, 7});

        ByteBuffer b = ByteBuffer.wrap(w.toByteArray());
        assertEquals((short) 0x1234, b.getShort());
        assertEquals(0x0A0B0C0D, b.getInt());
        assertEquals(1234567890123L, b.getLong());
        assertEquals("hello", Protocol.getString(b));
        assertNull(Protocol.getString(b));
        assertArrayEquals(new byte[]{1, 2, 3}, Protocol.getBytes(b));
        assertNull(Protocol.getBytes(b));
        assertArrayEquals(new int[]{5, 6, 7}, Protocol.getIntArray(b));
    }

    @Test
    void requestHeaderRoundTrip() {
        RequestHeader header = new RequestHeader((short) 3, (short) 0, 987, "client-x");
        ByteWriter w = new ByteWriter();
        header.writeTo(w);

        RequestHeader parsed = RequestHeader.parse(ByteBuffer.wrap(w.toByteArray()));
        assertEquals(3, parsed.apiKey);
        assertEquals(0, parsed.apiVersion);
        assertEquals(987, parsed.correlationId);
        assertEquals("client-x", parsed.clientId);
    }

    @Test
    void produceRequestRoundTrip() {
        ProduceRequest.Record r1 = new ProduceRequest.Record("k1".getBytes(), "v1".getBytes());
        ProduceRequest.Record r2 = new ProduceRequest.Record(null, "v2".getBytes());
        ProduceRequest.Partition part = new ProduceRequest.Partition(1, List.of(r1, r2));
        ProduceRequest.Topic topic = new ProduceRequest.Topic("orders", List.of(part));
        ProduceRequest request = new ProduceRequest((short) -1, 5000, Collections.singletonList(topic));

        ByteWriter w = new ByteWriter();
        request.writeTo(w);
        ProduceRequest parsed = ProduceRequest.parse(ByteBuffer.wrap(w.toByteArray()));

        assertEquals(-1, parsed.acks);
        assertEquals(5000, parsed.timeoutMs);
        assertEquals(1, parsed.topics.size());
        assertEquals("orders", parsed.topics.get(0).name);
        ProduceRequest.Partition parsedPart = parsed.topics.get(0).partitions.get(0);
        assertEquals(1, parsedPart.index);
        assertEquals(2, parsedPart.records.size());
        assertArrayEquals("v1".getBytes(), parsedPart.records.get(0).value);
        assertNull(parsedPart.records.get(1).key);
        assertArrayEquals("v2".getBytes(), parsedPart.records.get(1).value);
    }
}
package com.minikafka.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RecordAndIndexTest {

    @Test
    void recordRoundTrip() {
        Record record = new Record(42, 123456789L, "k".getBytes(), "v".getBytes());
        Record parsed = Record.readFrom(ByteBuffer.wrap(record.toBytes()));
        assertEquals(42, parsed.offset);
        assertEquals(123456789L, parsed.timestamp);
        assertEquals("k", parsed.keyAsString());
        assertEquals("v", parsed.valueAsString());
    }

    @Test
    void recordWithNullKey() {
        Record record = new Record(1, 1, null, "v".getBytes());
        Record parsed = Record.readFrom(ByteBuffer.wrap(record.toBytes()));
        assertNull(parsed.key);
        assertEquals("v", parsed.valueAsString());
    }

    @Test
    void corruptRecordIsDetected() {
        byte[] bytes = new Record(1, 1, null, "value".getBytes()).toBytes();
        bytes[bytes.length - 1] ^= 0xFF; // flip a byte of the value -> CRC no longer matches
        assertThrows(CorruptRecordException.class, () -> Record.readFrom(ByteBuffer.wrap(bytes)));
    }

    @Test
    void partialRecordReturnsNull() {
        byte[] bytes = new Record(1, 1, null, "value".getBytes()).toBytes();
        byte[] truncated = Arrays.copyOf(bytes, bytes.length - 3);
        assertNull(Record.readFrom(ByteBuffer.wrap(truncated)));
    }

    @Test
    void offsetIndexLookupAndPersistence(@TempDir Path dir) {
        Path path = dir.resolve("00000000000000000000.index");
        OffsetIndex index = new OffsetIndex(path);
        index.append(0, 0);
        index.append(10, 500);
        index.append(20, 1200);

        assertEquals(0, index.lookup(5));      // largest indexed offset <= 5 is 0 -> position 0
        assertEquals(500, index.lookup(10));
        assertEquals(500, index.lookup(15));
        assertEquals(1200, index.lookup(1000));
        index.flush();
        index.close();

        OffsetIndex reopened = new OffsetIndex(path);
        assertEquals(3, reopened.entries());
        assertEquals(1200, reopened.lookup(50));
        reopened.close();
    }
}
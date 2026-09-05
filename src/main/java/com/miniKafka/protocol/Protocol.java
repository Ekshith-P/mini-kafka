package com.minikafka.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Decoding helpers for the on-the-wire binary format. Requests arrive as a single frame which we
 * wrap in a {@link ByteBuffer} and read sequentially with these helpers. Responses are built with
 * {@link ByteWriter}.
 *
 * <p>All multi-byte integers are big-endian (network byte order) - {@link ByteBuffer}'s default.
 * The wire type system is small and Kafka-inspired:
 * <ul>
 *   <li>{@code INT8/INT16/INT32/INT64} - fixed-width signed integers</li>
 *   <li>{@code STRING / NULLABLE_STRING} - INT16 length prefix (-1 = null) then UTF-8 bytes</li>
 *   <li>{@code BYTES} - INT32 length prefix (-1 = null) then raw bytes</li>
 *   <li>{@code ARRAY} - INT32 element count (-1 = null) then that many elements</li>
 * </ul>
 */
public final class Protocol {

    private Protocol() {
    }

    public static String getString(ByteBuffer buf) {
        short len = buf.getShort();
        if (len < 0) {
            return null;
        }
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static byte[] getBytes(ByteBuffer buf) {
        int len = buf.getInt();
        if (len < 0) {
            return null;
        }
        byte[] bytes = new byte[len];
        buf.get(bytes);
        return bytes;
    }

    public static int[] getIntArray(ByteBuffer buf) {
        int count = buf.getInt();
        if (count < 0) {
            return null;
        }
        int[] values = new int[count];
        for (int i = 0; i < count; i++) {
            values[i] = buf.getInt();
        }
        return values;
    }
}
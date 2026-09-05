package com.minikafka.protocol;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * A small growable big-endian byte buffer for building responses (and requests on the client side).
 *
 * <p>Using this instead of a fixed {@link java.nio.ByteBuffer} means callers never have to compute
 * exact serialized sizes up front - a common source of off-by-one framing bugs. All integer writes
 * are big-endian to match {@link Protocol}'s decoding.
 */
public final class ByteWriter {
    private byte[] buf;
    private int pos;

    public ByteWriter() {
        this(64);
    }

    public ByteWriter(int initialCapacity) {
        this.buf = new byte[Math.max(16, initialCapacity)];
        this.pos = 0;
    }

    private void ensure(int extra) {
        if (pos + extra > buf.length) {
            int newCap = buf.length * 2;
            while (newCap < pos + extra) {
                newCap *= 2;
            }
            buf = Arrays.copyOf(buf, newCap);
        }
    }

    public ByteWriter putByte(int b) {
        ensure(1);
        buf[pos++] = (byte) b;
        return this;
    }

    public ByteWriter putBool(boolean v) {
        return putByte(v ? 1 : 0);
    }

    public ByteWriter putShort(int v) {
        ensure(2);
        buf[pos++] = (byte) (v >>> 8);
        buf[pos++] = (byte) v;
        return this;
    }

    public ByteWriter putInt(int v) {
        ensure(4);
        buf[pos++] = (byte) (v >>> 24);
        buf[pos++] = (byte) (v >>> 16);
        buf[pos++] = (byte) (v >>> 8);
        buf[pos++] = (byte) v;
        return this;
    }

    public ByteWriter putLong(long v) {
        ensure(8);
        buf[pos++] = (byte) (v >>> 56);
        buf[pos++] = (byte) (v >>> 48);
        buf[pos++] = (byte) (v >>> 40);
        buf[pos++] = (byte) (v >>> 32);
        buf[pos++] = (byte) (v >>> 24);
        buf[pos++] = (byte) (v >>> 16);
        buf[pos++] = (byte) (v >>> 8);
        buf[pos++] = (byte) v;
        return this;
    }

    /** Writes raw bytes with no length prefix. */
    public ByteWriter putRaw(byte[] bytes) {
        ensure(bytes.length);
        System.arraycopy(bytes, 0, buf, pos, bytes.length);
        pos += bytes.length;
        return this;
    }

    /** STRING / NULLABLE_STRING: INT16 length (-1 = null) then UTF-8 bytes. */
    public ByteWriter putString(String s) {
        if (s == null) {
            return putShort(-1);
        }
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("string too long: " + bytes.length);
        }
        putShort(bytes.length);
        return putRaw(bytes);
    }

    /** BYTES: INT32 length (-1 = null) then raw bytes. */
    public ByteWriter putBytes(byte[] value) {
        if (value == null) {
            return putInt(-1);
        }
        putInt(value.length);
        return putRaw(value);
    }

    public ByteWriter putIntArray(int[] values) {
        if (values == null) {
            return putInt(-1);
        }
        putInt(values.length);
        for (int v : values) {
            putInt(v);
        }
        return this;
    }

    public int size() {
        return pos;
    }

    public byte[] toByteArray() {
        return Arrays.copyOf(buf, pos);
    }
}
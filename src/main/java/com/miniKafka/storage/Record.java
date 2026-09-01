package com.minikafka.storage;

import com.minikafka.protocol.ByteWriter;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/**
 * A single stored message plus the metadata the log assigns to it.
 *
 * <p>On-disk layout of one record entry (all integers big-endian):
 * <pre>
 * length    INT32  number of bytes that follow this field
 * crc       INT32  CRC32 over everything after this field (offset..value)
 * offset    INT64  the record's absolute offset in the partition
 * timestamp INT64  broker append time (epoch millis)
 * key       BYTES  INT32 length (-1 = null) + bytes
 * value     BYTES  INT32 length (-1 = null) + bytes
 * </pre>
 *
 * The CRC lets us detect torn writes and bit-rot: on read we recompute it and reject mismatches,
 * and during crash recovery a mismatch marks where a partial write was truncated.
 */
public final class Record {
    /** Bytes of fixed overhead per record entry: length + crc + offset + timestamp. */
    static final int OVERHEAD = 4 + 4 + 8 + 8;

    public final long offset;
    public final long timestamp;
    public final byte[] key;   // nullable
    public final byte[] value; // nullable

    public Record(long offset, long timestamp, byte[] key, byte[] value) {
        this.offset = offset;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
    }

    /** Total number of bytes this record occupies on disk. */
    public int sizeInBytes() {
        int keyLen = 4 + (key == null ? 0 : key.length);
        int valueLen = 4 + (value == null ? 0 : value.length);
        return OVERHEAD + keyLen + valueLen;
    }

    /** Serializes the full on-disk entry (including the length prefix and CRC) into {@code out}. */
    public void writeTo(ByteWriter out) {
        ByteWriter payload = new ByteWriter(sizeInBytes());
        payload.putLong(offset);
        payload.putLong(timestamp);
        payload.putBytes(key);
        payload.putBytes(value);
        byte[] payloadBytes = payload.toByteArray();

        CRC32 crc = new CRC32();
        crc.update(payloadBytes);

        out.putInt(4 + payloadBytes.length); // length = crc field + payload
        out.putInt((int) crc.getValue());
        out.putRaw(payloadBytes);
    }

    public byte[] toBytes() {
        ByteWriter out = new ByteWriter(sizeInBytes());
        writeTo(out);
        return out.toByteArray();
    }

    /**
     * Reads one record from {@code buf} at its current position.
     *
     * @return the record, or {@code null} if there are not enough bytes for a complete entry
     *         (i.e. a partial trailing record)
     * @throws CorruptRecordException if a complete entry is present but its CRC does not match
     */
    public static Record readFrom(ByteBuffer buf) {
        if (buf.remaining() < 4) {
            return null;
        }
        int start = buf.position();
        int length = buf.getInt();
        if (length < 4 + 8 + 8 + 4 + 4) { // crc + offset + timestamp + empty key + empty value
            // Not a plausible record; treat as end/corruption.
            buf.position(start);
            return null;
        }
        if (buf.remaining() < length) {
            buf.position(start);
            return null; // partial trailing record
        }
        int crcStored = buf.getInt();
        byte[] payload = new byte[length - 4];
        buf.get(payload);

        CRC32 crc = new CRC32();
        crc.update(payload);
        if ((int) crc.getValue() != crcStored) {
            throw new CorruptRecordException("CRC mismatch at buffer offset " + start);
        }

        ByteBuffer p = ByteBuffer.wrap(payload);
        long offset = p.getLong();
        long timestamp = p.getLong();
        byte[] key = readNullableBytes(p);
        byte[] value = readNullableBytes(p);
        return new Record(offset, timestamp, key, value);
    }

    private static byte[] readNullableBytes(ByteBuffer buf) {
        int len = buf.getInt();
        if (len < 0) {
            return null;
        }
        byte[] out = new byte[len];
        buf.get(out);
        return out;
    }

    public String valueAsString() {
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    public String keyAsString() {
        return key == null ? null : new String(key, StandardCharsets.UTF_8);
    }
}
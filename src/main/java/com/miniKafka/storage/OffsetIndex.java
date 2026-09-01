package com.minikafka.storage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

/**
 * A sparse index from relative offset to byte position within a {@link LogSegment}.
 *
 * <p>Instead of indexing every record, we add an entry roughly every {@code indexIntervalBytes} of
 * appended data. To find a record we binary-search this small in-memory index for the largest
 * indexed offset {@code <=} the target (O(log n)), then scan forward in the log from that position.
 * This is the same trick Apache Kafka uses to keep reads fast without an index entry per message.
 *
 * <p>On disk each entry is two big-endian INT32s: {@code (relativeOffset, position)}. Relative
 * offsets and positions both fit in 32 bits because a segment is bounded well under 2 GiB.
 */
public final class OffsetIndex {
    private static final int ENTRY_SIZE = 8; // two INT32s

    private final Path path;
    private final FileChannel channel;

    private int[] relOffsets;
    private int[] positions;
    private int count;

    public OffsetIndex(Path path) {
        this.path = path;
        this.relOffsets = new int[512];
        this.positions = new int[512];
        this.count = 0;
        try {
            this.channel = FileChannel.open(path,
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            load();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to open index " + path, e);
        }
    }

    private void load() throws IOException {
        long size = channel.size();
        long usable = size - (size % ENTRY_SIZE); // ignore any torn trailing entry
        ByteBuffer buf = ByteBuffer.allocate((int) usable);
        int read = 0;
        while (read < usable) {
            int n = channel.read(buf, read);
            if (n < 0) {
                break;
            }
            read += n;
        }
        buf.flip();
        while (buf.remaining() >= ENTRY_SIZE) {
            ensureCapacity();
            relOffsets[count] = buf.getInt();
            positions[count] = buf.getInt();
            count++;
        }
        channel.position(usable);
    }

    private void ensureCapacity() {
        if (count == relOffsets.length) {
            relOffsets = Arrays.copyOf(relOffsets, relOffsets.length * 2);
            positions = Arrays.copyOf(positions, positions.length * 2);
        }
    }

    /** Records that the record with this relative offset begins at {@code position} in the log. */
    public void append(int relativeOffset, int position) {
        if (count > 0 && relativeOffset <= relOffsets[count - 1]) {
            return; // indexes must be strictly increasing; ignore duplicates/regressions
        }
        ensureCapacity();
        relOffsets[count] = relativeOffset;
        positions[count] = position;
        count++;
        try {
            ByteBuffer buf = ByteBuffer.allocate(ENTRY_SIZE);
            buf.putInt(relativeOffset);
            buf.putInt(position);
            buf.flip();
            while (buf.hasRemaining()) {
                channel.write(buf);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to append to index " + path, e);
        }
    }

    /**
     * Returns the largest indexed byte position whose relative offset is {@code <= target}. If no
     * such entry exists the scan should start at the beginning of the segment, so returns 0.
     */
    public int lookup(int targetRelativeOffset) {
        int lo = 0;
        int hi = count - 1;
        int result = 0;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (relOffsets[mid] <= targetRelativeOffset) {
                result = positions[mid];
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return result;
    }

    public int entries() {
        return count;
    }

    /** Discards all entries - used when rebuilding the index during recovery. */
    public void reset() {
        count = 0;
        try {
            channel.truncate(0);
            channel.position(0);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to reset index " + path, e);
        }
    }

    public void flush() {
        if (!channel.isOpen()) {
            return;
        }
        try {
            channel.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to flush index " + path, e);
        }
    }

    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to close index " + path, e);
        }
    }
}
package com.minikafka.storage;

import com.minikafka.common.Log;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * One segment of a partition's commit log: an append-only {@code .log} file plus its sparse
 * {@code .index}. A partition log is a sequence of these segments, ordered by the offset of their
 * first record (e.g. {@code 00000000000000000000.log}) - the same scheme Kafka uses.
 *
 * <p>All I/O uses positional {@link FileChannel} reads/writes so it never depends on a shared file
 * pointer. Thread-safety is provided by the owning {@link Log}'s read-write lock: appends run under
 * the write lock, reads under the read lock.
 */
public final class LogSegment {
    private static final Log LOG = Log.of(LogSegment.class);

    private final long baseOffset;
    private final Path logPath;
    private final FileChannel channel;
    private final OffsetIndex index;
    private final int indexIntervalBytes;

    private volatile long size;       // bytes written so far (also the append position)
    private int bytesSinceLastIndex;

    public LogSegment(Path dir, long baseOffset, int indexIntervalBytes) {
        this.baseOffset = baseOffset;
        this.indexIntervalBytes = indexIntervalBytes;

        String name = String.format("%020d", baseOffset);
        this.logPath = dir.resolve(name + ".log");
        Path indexPath = dir.resolve(name + ".index");

        try {
            this.channel = FileChannel.open(
                    logPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE
            );
            this.size = channel.size();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to open segment " + logPath, e);
        }

        this.index = new OffsetIndex(indexPath);
        this.bytesSinceLastIndex = 0;
    }

    public long baseOffset() {
        return baseOffset;
    }

    public long sizeInBytes() {
        return size;
    }

    /** Appends a record at the end of the segment. Caller must hold the partition write lock. */
    public void append(Record record) {
        byte[] bytes = record.toBytes();
        int position = (int) size;
        int relativeOffset = (int) (record.offset - baseOffset);

        if (index.entries() == 0 || bytesSinceLastIndex >= indexIntervalBytes) {
            index.append(relativeOffset, position);
            bytesSinceLastIndex = 0;
        }

        try {
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            long writePos = size;

            while (buf.hasRemaining()) {
                writePos += channel.write(buf, writePos);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to append to " + logPath, e);
        }

        size += bytes.length;
        bytesSinceLastIndex += bytes.length;
    }

    /**
     * Reads records with offset in {@code [startOffset, maxOffsetExclusive)} up to roughly
     * {@code maxBytes} of record data. At least one record is returned if one qualifies, even if it
     * alone exceeds {@code maxBytes}, so a consumer can never get permanently stuck on a large
     * record. Caller must hold the partition read lock.
     */
    public List<Record> read(long startOffset, int maxBytes, long maxOffsetExclusive) {
        List<Record> out = new ArrayList<>();
        if (startOffset >= maxOffsetExclusive) {
            return out;
        }

        long pos = index.lookup((int) (startOffset - baseOffset));
        int accumulated = 0;

        while (pos < size) {
            Record record = readRecordAt(pos);
            if (record == null) {
                break; // reached a partial trailing record / end of data
            }

            int recSize = record.sizeInBytes();

            if (record.offset >= maxOffsetExclusive) {
                break;
            }

            if (record.offset >= startOffset) {
                out.add(record);
                accumulated += recSize;

                if (accumulated >= maxBytes) {
                    break; // already returned at least one record
                }
            }

            pos += recSize;
        }

        return out;
    }

    private Record readRecordAt(long pos) {
        ByteBuffer lenBuf = readRegion(pos, 4);
        if (lenBuf == null) {
            return null;
        }

        int length = lenBuf.getInt();
        if (length <= 0) {
            return null;
        }

        ByteBuffer recBuf = readRegion(pos, 4 + length);
        if (recBuf == null) {
            return null;
        }

        return Record.readFrom(recBuf); // may throw CorruptRecordException
    }

    /** Positionally reads exactly {@code len} bytes at {@code pos}, or null if fewer are available. */
    private ByteBuffer readRegion(long pos, int len) {
        try {
            if (pos + len > size) {
                return null;
            }

            ByteBuffer buf = ByteBuffer.allocate(len);
            int read = 0;

            while (read < len) {
                int n = channel.read(buf, pos + read);
                if (n < 0) {
                    return null;
                }
                read += n;
            }

            buf.flip();
            return buf;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read " + logPath, e);
        }
    }

    /**
     * Rescans the segment from the start, rebuilding the index and validating CRCs. If a partial or
     * corrupt record is found (e.g. after a crash mid-write), the file is truncated to the last good
     * record. Returns the offset of the last valid record, or {@code baseOffset - 1} if empty.
     */
    public long recover() {
        index.reset();
        bytesSinceLastIndex = 0;

        long pos = 0;
        long lastOffset = baseOffset - 1;

        while (true) {
            Record record;

            try {
                record = readRecordAt(pos);
            } catch (CorruptRecordException e) {
                LOG.warn(
                        "corrupt record in {} at position {}, truncating",
                        logPath,
                        pos
                );
                break;
            }

            if (record == null) {
                break;
            }

            int recSize = record.sizeInBytes();
            int relativeOffset = (int) (record.offset - baseOffset);

            if (index.entries() == 0 || bytesSinceLastIndex >= indexIntervalBytes) {
                index.append(relativeOffset, (int) pos);
                bytesSinceLastIndex = 0;
            }

            bytesSinceLastIndex += recSize;
            lastOffset = record.offset;
            pos += recSize;
        }

        if (pos < size) {
            try {
                channel.truncate(pos);
            } catch (IOException e) {
                throw new UncheckedIOException("failed to truncate " + logPath, e);
            }
            size = pos;
        }

        index.flush();
        return lastOffset;
    }

    public void flush() {
        if (!channel.isOpen()) {
            return; // already closed (e.g. a background flush raced with shutdown)
        }

        try {
            channel.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to flush " + logPath, e);
        }

        index.flush();
    }

    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to close " + logPath, e);
        }

        index.close();
    }
}
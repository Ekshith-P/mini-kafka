package com.minikafka.storage;

import com.minikafka.common.Errors;
import com.minikafka.common.TopicPartition;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

/**
 * The append-only commit log for a single partition: an ordered list of {@link LogSegment}s that
 * behaves as one logically infinite sequence of records addressed by offset.
 *
 * <h2>Offsets</h2>
 * <ul>
 *   <li><b>Log-end offset (LEO)</b> = {@link #logEndOffset()} - the offset the next appended record
 *       will get; equivalently, one past the last record.</li>
 *   <li><b>High-watermark (HW)</b> = {@link #highWatermark()} - the exclusive upper bound of
 *       <i>committed</i> records. Consumers may only read below the HW. On a single replica the HW
 *       tracks the LEO; with replication the leader advances it only once the in-sync followers have
 *       copied the data (see the replication package).</li>
 * </ul>
 *
 * <h2>Concurrency</h2>
 * A per-partition {@link ReentrantReadWriteLock} serializes appends (write lock) while allowing
 * concurrent reads (read lock). Different partitions use different locks, so throughput scales with
 * the number of partitions.
 */
public final class Log {
    private static final com.minikafka.common.Log LOG =
            com.minikafka.common.Log.of(Log.class);

    private final TopicPartition tp;
    private final Path dir;
    private final long segmentBytes;
    private final int indexIntervalBytes;
    private final Path hwCheckpoint;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final List<LogSegment> segments = new ArrayList<>();

    private volatile long nextOffset;       // log-end offset (LEO)
    private volatile long highWatermark;    // exclusive upper bound of committed records
    private volatile boolean autoAdvanceHighWatermark = true;

    public Log(Path dir, TopicPartition tp, long segmentBytes, int indexIntervalBytes) {
        this.tp = tp;
        this.dir = dir;
        this.segmentBytes = segmentBytes;
        this.indexIntervalBytes = indexIntervalBytes;
        this.hwCheckpoint = dir.resolve("highwatermark");

        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to create log dir " + dir, e);
        }

        loadSegments();
        recoverActiveSegment();
        restoreHighWatermark();

        LOG.info("loaded partition {} with LEO={} HW={} ({} segment(s))",
                tp, nextOffset, highWatermark, segments.size());
    }

    private void loadSegments() {
        List<Long> baseOffsets = new ArrayList<>();

        try (Stream<Path> files = Files.list(dir)) {
            files.filter(p -> p.getFileName().toString().endsWith(".log"))
                    .forEach(p -> {
                        String name = p.getFileName().toString();
                        String base = name.substring(0, name.length() - ".log".length());
                        baseOffsets.add(Long.parseLong(base));
                    });
        } catch (IOException e) {
            throw new UncheckedIOException("failed to list log dir " + dir, e);
        }

        baseOffsets.sort(Long::compareTo);

        for (long base : baseOffsets) {
            segments.add(new LogSegment(dir, base, indexIntervalBytes));
        }

        if (segments.isEmpty()) {
            segments.add(new LogSegment(dir, 0, indexIntervalBytes));
        }
    }

    private void recoverActiveSegment() {
        LogSegment active = activeSegment();
        long lastOffset = active.recover();
        nextOffset = lastOffset + 1;
    }

    private void restoreHighWatermark() {
        long hw = 0;

        try {
            if (Files.exists(hwCheckpoint)) {
                hw = Long.parseLong(Files.readString(hwCheckpoint).trim());
            }
        } catch (IOException | NumberFormatException e) {
            LOG.warn("could not read HW checkpoint for {}: {}", tp, e.toString());
        }

        if (autoAdvanceHighWatermark) {
            hw = nextOffset; // single replica: everything on disk is committed
        }

        highWatermark = Math.min(Math.max(hw, 0), nextOffset);
    }

    // --- append paths -----------------------------------------------------

    /** Leader append: assigns the next offset to a new record. Returns the assigned offset. */
    public long append(byte[] key, byte[] value) {
        lock.writeLock().lock();

        try {
            long offset = nextOffset;
            Record record = new Record(offset, System.currentTimeMillis(), key, value);

            maybeRoll(record.sizeInBytes());
            activeSegment().append(record);

            nextOffset = offset + 1;

            if (autoAdvanceHighWatermark) {
                highWatermark = nextOffset;
            }

            return offset;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Follower append: stores records that already carry leader-assigned offsets, in order. Records
     * with offset below the current LEO are treated as duplicates and skipped.
     */
    public void appendAsFollower(List<Record> records) {
        lock.writeLock().lock();

        try {
            for (Record record : records) {
                if (record.offset < nextOffset) {
                    continue; // already have it
                }

                if (record.offset > nextOffset) {
                    // A gap means we missed records; refuse rather than corrupt the log.
                    throw new IllegalStateException(
                            "offset gap on " + tp
                                    + ": expected " + nextOffset
                                    + " got " + record.offset);
                }

                maybeRoll(record.sizeInBytes());
                activeSegment().append(record);
                nextOffset = record.offset + 1;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    // --- read path --------------------------------------------------------

    /**
     * Reads up to {@code maxBytes} of records starting at {@code fetchOffset}.
     *
     * @param forFollower if true this is a replica fetch and may read up to the LEO; otherwise it is
     *                    a consumer fetch and is capped at the high-watermark
     */
    public LogReadResult read(long fetchOffset, int maxBytes, boolean forFollower) {
        lock.readLock().lock();

        try {
            long leo = nextOffset;
            long hw = highWatermark;
            long logStart = segments.get(0).baseOffset();

            if (fetchOffset < logStart || fetchOffset > leo) {
                return LogReadResult.error(
                        Errors.OFFSET_OUT_OF_RANGE, hw, logStart);
            }

            long maxExclusive = forFollower ? leo : hw;
            LogSegment segment = segmentForOffset(fetchOffset);
            List<Record> records =
                    segment.read(fetchOffset, maxBytes, maxExclusive);

            return new LogReadResult(records, hw, logStart, Errors.NONE);
        } finally {
            lock.readLock().unlock();
        }
    }

    private LogSegment segmentForOffset(long offset) {
        for (int i = segments.size() - 1; i >= 0; i--) {
            if (segments.get(i).baseOffset() <= offset) {
                return segments.get(i);
            }
        }

        return segments.get(0);
    }

    private void maybeRoll(int incomingSize) {
        LogSegment active = activeSegment();

        if (active.sizeInBytes() > 0
                && active.sizeInBytes() + incomingSize > segmentBytes) {
            LogSegment rolled =
                    new LogSegment(dir, nextOffset, indexIntervalBytes);
            segments.add(rolled);

            LOG.info("rolled {} to new segment at base offset {}",
                    tp, nextOffset);
        }
    }

    private LogSegment activeSegment() {
        return segments.get(segments.size() - 1);
    }

    // --- offsets ----------------------------------------------------------

    public long logEndOffset() {
        return nextOffset;
    }

    public long logStartOffset() {
        lock.readLock().lock();

        try {
            return segments.get(0).baseOffset();
        } finally {
            lock.readLock().unlock();
        }
    }

    public long highWatermark() {
        return highWatermark;
    }

    public void setAutoAdvanceHighWatermark(boolean value) {
        this.autoAdvanceHighWatermark = value;

        if (value) {
            highWatermark = Math.max(highWatermark, nextOffset);
        }
    }

    /** Advances the high-watermark, never past the LEO and never backwards. */
    public void updateHighWatermark(long proposed) {
        lock.writeLock().lock();

        try {
            long clamped = Math.min(proposed, nextOffset);

            if (clamped > highWatermark) {
                highWatermark = clamped;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    // --- lifecycle --------------------------------------------------------

    public void flush() {
        lock.readLock().lock();

        try {
            for (LogSegment segment : segments) {
                segment.flush();
            }

            checkpointHighWatermark();
        } finally {
            lock.readLock().unlock();
        }
    }

    private void checkpointHighWatermark() {
        try {
            Files.writeString(
                    hwCheckpoint,
                    Long.toString(highWatermark));
        } catch (IOException e) {
            LOG.warn("failed to checkpoint HW for {}: {}",
                    tp, e.toString());
        }
    }

    public void close() {
        lock.writeLock().lock();

        try {
            checkpointHighWatermark();

            for (LogSegment segment : segments) {
                segment.close();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public TopicPartition topicPartition() {
        return tp;
    }
}
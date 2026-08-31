package com.minikafka.broker;

import com.minikafka.common.Log;
import com.minikafka.common.TopicPartition;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Durable store of committed consumer-group offsets.
 *
 * <p>Backed by a simple append-only file: each commit appends a line, and on startup the file is
 * replayed with last-write-wins to rebuild the in-memory map. This is a miniature of how Kafka stores
 * offsets in its compacted internal {@code __consumer_offsets} topic (log compaction to bound growth
 * is left as future work).
 */
public final class OffsetManager {
    private static final Log LOG = Log.of(OffsetManager.class);

    public static final class OffsetAndMetadata {
        public final long offset;
        public final String metadata; // nullable

        public OffsetAndMetadata(long offset, String metadata) {
            this.offset = offset;
            this.metadata = metadata;
        }
    }

    private final Path file;
    private final Map<String, OffsetAndMetadata> offsets = new ConcurrentHashMap<>();
    private final Object writeLock = new Object();

    public OffsetManager(Path file) {
        this.file = file;
        load();
    }

    private static String key(String group, TopicPartition tp) {
        return group + '\t' + tp.topic() + '\t' + tp.partition();
    }

    private void load() {
        if (!Files.exists(file)) {
            return;
        }
        try {
            List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
            for (String line : lines) {
                if (line.isBlank()) {
                    continue;
                }
                String[] parts = line.split("\t", -1);
                if (parts.length < 4) {
                    continue;
                }
                String group = parts[0];
                String topic = parts[1];
                int partition = Integer.parseInt(parts[2]);
                long offset = Long.parseLong(parts[3]);
                String metadata = parts.length >= 5 && !parts[4].isEmpty() ? parts[4] : null;
                offsets.put(key(group, new TopicPartition(topic, partition)),
                        new OffsetAndMetadata(offset, metadata));
            }
            LOG.info("recovered {} committed offset(s) from {}", offsets.size(), file);
        } catch (IOException | NumberFormatException e) {
            LOG.warn("failed to load committed offsets from {}: {}", file, e.toString());
        }
    }

    public void commit(String group, TopicPartition tp, long offset, String metadata) {
        offsets.put(key(group, tp), new OffsetAndMetadata(offset, metadata));
        String line = group + '\t' + tp.topic() + '\t' + tp.partition() + '\t' + offset + '\t'
                + (metadata == null ? "" : metadata) + '\n';
        synchronized (writeLock) {
            try (BufferedWriter w = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
                w.write(line);
            } catch (IOException e) {
                throw new UncheckedIOException("failed to persist offset commit", e);
            }
        }
    }

    /** Returns the committed offset/metadata for the group+partition, or null if none. */
    public OffsetAndMetadata fetch(String group, TopicPartition tp) {
        return offsets.get(key(group, tp));
    }
}
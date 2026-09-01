package com.minikafka.storage;

import com.minikafka.common.TopicPartition;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Owns every partition {@link Log} on this broker and maps each {@link TopicPartition} to its log
 * directory under {@code dataDir}. Partition directories are named {@code <topic>-<partition>},
 * matching Kafka's on-disk layout.
 *
 * <p>Note: within this package the unqualified name {@code Log} refers to the partition commit log
 * ({@link Log}); the logging utility is referenced by its full name
 * {@code com.minikafka.common.Log} to avoid the clash.
 */
public final class LogManager {
    private static final com.minikafka.common.Log LOG = com.minikafka.common.Log.of(LogManager.class);

    private final Path dataDir;
    private final long segmentBytes;
    private final int indexIntervalBytes;
    private final Map<TopicPartition, Log> logs = new ConcurrentHashMap<>();

    public LogManager(Path dataDir, long segmentBytes, int indexIntervalBytes) {
        this.dataDir = dataDir;
        this.segmentBytes = segmentBytes;
        this.indexIntervalBytes = indexIntervalBytes;
        try {
            Files.createDirectories(dataDir);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to create data dir " + dataDir, e);
        }
        loadExisting();
    }

    private void loadExisting() {
        try (Stream<Path> dirs = Files.list(dataDir)) {
            dirs.filter(Files::isDirectory).forEach(dir -> {
                TopicPartition tp = parsePartitionDir(dir.getFileName().toString());
                if (tp != null) {
                    logs.put(tp, new Log(dir, tp, segmentBytes, indexIntervalBytes));
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException("failed to scan data dir " + dataDir, e);
        }
        LOG.info("recovered {} partition(s) from {}", logs.size(), dataDir);
    }

    /** Parses {@code <topic>-<partition>}, splitting on the last '-' so topics may contain '-'. */
    static TopicPartition parsePartitionDir(String name) {
        int dash = name.lastIndexOf('-');
        if (dash <= 0 || dash == name.length() - 1) {
            return null;
        }
        String topic = name.substring(0, dash);
        try {
            int partition = Integer.parseInt(name.substring(dash + 1));
            return new TopicPartition(topic, partition);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    public Log getOrCreateLog(TopicPartition tp) {
        return logs.computeIfAbsent(tp, key -> {
            Path dir = dataDir.resolve(key.topic() + "-" + key.partition());
            LOG.info("creating log for {}", key);
            return new Log(dir, key, segmentBytes, indexIntervalBytes);
        });
    }

    public Log getLog(TopicPartition tp) {
        return logs.get(tp);
    }

    public boolean hasLog(TopicPartition tp) {
        return logs.containsKey(tp);
    }

    public List<TopicPartition> allPartitions() {
        return new ArrayList<>(logs.keySet());
    }

    public void flushAll() {
        for (Log log : logs.values()) {
            log.flush();
        }
    }

    public void closeAll() {
        for (Log log : logs.values()) {
            log.close();
        }
        LOG.info("closed {} partition log(s)", logs.size());
    }
}
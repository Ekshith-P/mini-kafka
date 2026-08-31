package com.minikafka.broker;

import com.minikafka.common.Node;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * All tunables for a single broker, loaded from a {@code .properties} file. The same file (differing
 * only in {@code broker.id}/{@code listeners.port}/{@code log.dirs}) is used by every broker in a
 * cluster; {@code cluster.brokers} and {@code topics} must match across them so their derived
 * partition assignment agrees.
 */
public final class BrokerConfig {
    private final int brokerId;
    private final String host;
    private final int port;
    private final Path logDir;
    private final long segmentBytes;
    private final int indexIntervalBytes;
    private final List<Node> clusterNodes;
    private final List<TopicConfig> topics;
    private final int replicaFetchIntervalMs;
    private final int replicaFetchMaxBytes;
    private final long replicaLagTimeMaxMs;
    private final int produceTimeoutMs;
    private final int ioThreads;
    private final boolean raftEnabled;
    private final Path raftDir;
    private final int raftElectionMinMs;
    private final int raftElectionMaxMs;
    private final int raftHeartbeatMs;
    private final long raftLivenessTimeoutMs;
    private final int raftConnectTimeoutMs;
    private final int raftReadTimeoutMs;
    private final int groupRebalanceTimeoutMs;

    private BrokerConfig(Properties p) {
        this.brokerId = requireInt(p, "broker.id");
        this.host = p.getProperty("listeners.host", "localhost").trim();
        this.port = requireInt(p, "listeners.port");
        this.logDir = Path.of(p.getProperty("log.dirs", "data/broker-" + brokerId).trim());
        this.segmentBytes = Long.parseLong(p.getProperty("log.segment.bytes", "1048576").trim());
        this.indexIntervalBytes = Integer.parseInt(p.getProperty("log.index.interval.bytes", "4096").trim());
        this.clusterNodes = parseNodes(p.getProperty("cluster.brokers", ""));
        this.topics = parseTopics(p.getProperty("topics", ""));
        this.replicaFetchIntervalMs = Integer.parseInt(p.getProperty("replica.fetch.interval.ms", "100").trim());
        this.replicaFetchMaxBytes = Integer.parseInt(p.getProperty("replica.fetch.max.bytes", "1048576").trim());
        this.replicaLagTimeMaxMs = Long.parseLong(p.getProperty("replica.lag.time.max.ms", "10000").trim());
        this.produceTimeoutMs = Integer.parseInt(p.getProperty("produce.timeout.ms", "30000").trim());
        this.ioThreads = Integer.parseInt(p.getProperty("num.io.threads",
                String.valueOf(Runtime.getRuntime().availableProcessors())).trim());

        // Raft controller quorum (used for automatic partition-leader failover).
        String raftEnabledProp = p.getProperty("raft.enabled");
        this.raftEnabled = raftEnabledProp != null
                ? Boolean.parseBoolean(raftEnabledProp.trim())
                : clusterNodes.size() > 1; // pointless on a single broker
        this.raftDir = Path.of(p.getProperty("raft.dir", logDir.resolve("raft").toString()).trim());
        this.raftElectionMinMs = Integer.parseInt(p.getProperty("raft.election.min.ms", "400").trim());
        this.raftElectionMaxMs = Integer.parseInt(p.getProperty("raft.election.max.ms", "800").trim());
        this.raftHeartbeatMs = Integer.parseInt(p.getProperty("raft.heartbeat.ms", "100").trim());
        this.raftLivenessTimeoutMs = Long.parseLong(p.getProperty("raft.liveness.timeout.ms", "2000").trim());
        this.raftConnectTimeoutMs = Integer.parseInt(p.getProperty("raft.connect.timeout.ms", "1000").trim());
        this.raftReadTimeoutMs = Integer.parseInt(p.getProperty("raft.read.timeout.ms", "2000").trim());
        this.groupRebalanceTimeoutMs = Integer.parseInt(
                p.getProperty("group.rebalance.timeout.ms", "3000").trim());
    }

    public static BrokerConfig fromFile(Path path) {
        Properties p = new Properties();
        try (InputStream in = Files.newInputStream(path)) {
            p.load(in);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read config " + path, e);
        }
        return new BrokerConfig(p);
    }

    public static BrokerConfig fromProperties(Properties p) {
        return new BrokerConfig(p);
    }

    private static int requireInt(Properties p, String key) {
        String v = p.getProperty(key);
        if (v == null) {
            throw new IllegalArgumentException("missing required config: " + key);
        }
        return Integer.parseInt(v.trim());
    }

    private List<Node> parseNodes(String raw) {
        List<Node> nodes = new ArrayList<>();
        if (raw != null && !raw.isBlank()) {
            for (String part : raw.split(",")) {
                String[] hp = part.trim().split(":");
                if (hp.length != 3) {
                    throw new IllegalArgumentException("bad cluster.brokers entry: " + part
                            + " (expected id:host:port)");
                }
                nodes.add(new Node(Integer.parseInt(hp[0].trim()), hp[1].trim(),
                        Integer.parseInt(hp[2].trim())));
            }
        }
        if (nodes.isEmpty()) {
            nodes.add(new Node(brokerId, host, port)); // single-broker cluster
        }
        return Collections.unmodifiableList(nodes);
    }

    private List<TopicConfig> parseTopics(String raw) {
        List<TopicConfig> result = new ArrayList<>();
        if (raw != null && !raw.isBlank()) {
            for (String part : raw.split(",")) {
                String[] fields = part.trim().split(":");
                if (fields.length != 3) {
                    throw new IllegalArgumentException("bad topics entry: " + part
                            + " (expected name:partitions:replicationFactor)");
                }
                result.add(new TopicConfig(fields[0].trim(),
                        Integer.parseInt(fields[1].trim()), Integer.parseInt(fields[2].trim())));
            }
        }
        return Collections.unmodifiableList(result);
    }

    public int brokerId() {
        return brokerId;
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    public Path logDir() {
        return logDir;
    }

    public long segmentBytes() {
        return segmentBytes;
    }

    public int indexIntervalBytes() {
        return indexIntervalBytes;
    }

    public List<Node> clusterNodes() {
        return clusterNodes;
    }

    public List<TopicConfig> topics() {
        return topics;
    }

    public int replicaFetchIntervalMs() {
        return replicaFetchIntervalMs;
    }

    public int replicaFetchMaxBytes() {
        return replicaFetchMaxBytes;
    }

    public long replicaLagTimeMaxMs() {
        return replicaLagTimeMaxMs;
    }

    public int produceTimeoutMs() {
        return produceTimeoutMs;
    }

    public int ioThreads() {
        return ioThreads;
    }

    public boolean raftEnabled() {
        return raftEnabled;
    }

    public Path raftDir() {
        return raftDir;
    }

    public int raftElectionMinMs() {
        return raftElectionMinMs;
    }

    public int raftElectionMaxMs() {
        return raftElectionMaxMs;
    }

    public int raftHeartbeatMs() {
        return raftHeartbeatMs;
    }

    public long raftLivenessTimeoutMs() {
        return raftLivenessTimeoutMs;
    }

    public int raftConnectTimeoutMs() {
        return raftConnectTimeoutMs;
    }

    public int raftReadTimeoutMs() {
        return raftReadTimeoutMs;
    }

    public int groupRebalanceTimeoutMs() {
        return groupRebalanceTimeoutMs;
    }
}
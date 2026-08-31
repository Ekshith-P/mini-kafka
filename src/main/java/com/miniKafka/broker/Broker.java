package com.minikafka.broker;

import com.minikafka.common.Log;
import com.minikafka.common.Node;
import com.minikafka.metrics.Metrics;
import com.minikafka.network.KafkaServer;
import com.minikafka.raft.FilePersistence;
import com.minikafka.raft.RaftConfig;
import com.minikafka.raft.RaftNode;
import com.minikafka.replication.ReplicaManager;
import com.minikafka.storage.LogManager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Wires together and owns the lifecycle of every broker component: storage ({@link LogManager}), the
 * cluster view ({@link Cluster}), replication ({@link ReplicaManager}), consumer offsets
 * ({@link OffsetManager}), request handling ({@link RequestHandler}), the network server
 * ({@link KafkaServer}), and — when the cluster has more than one broker — the KRaft-style control
 * plane: a {@link RaftNode} quorum plus a {@link Controller} that fails partition leaders over when a
 * broker dies.
 */
public final class Broker {
    private static final Log LOG = Log.of(Broker.class);

    private final BrokerConfig config;
    private final Metrics metrics = new Metrics();
    private final LogManager logManager;
    private final Cluster cluster;
    private final OffsetManager offsetManager;
    private final ReplicaManager replicaManager;
    private final GroupCoordinator groupCoordinator;
    private final KafkaServer server;

    // Control plane (null when raft is disabled, i.e. a single-broker cluster).
    private final RaftNode raftNode;
    private final Controller controller;
    private final SocketRaftTransport raftTransport;

    private ScheduledExecutorService background;
    private volatile boolean closed;

    public Broker(BrokerConfig config) {
        this.config = config;
        this.logManager = new LogManager(config.logDir(), config.segmentBytes(), config.indexIntervalBytes());
        this.cluster = new Cluster(config.clusterNodes());
        for (TopicConfig topic : config.topics()) {
            cluster.addOrUpdateTopic(topic);
        }
        this.offsetManager = new OffsetManager(config.logDir().resolve("_consumer_offsets.log"));
        this.replicaManager = new ReplicaManager(config.brokerId(), logManager, cluster, config);
        this.groupCoordinator = new GroupCoordinator(cluster, config.groupRebalanceTimeoutMs());
        RequestHandler handler = new RequestHandler(config.brokerId(), logManager::getOrCreateLog,
                replicaManager, cluster, offsetManager, metrics, config, groupCoordinator);
        this.server = new KafkaServer(config.port(), handler, metrics);

        if (config.raftEnabled()) {
            Map<Integer, Node> peerNodes = new HashMap<>();
            List<Integer> peerIds = new ArrayList<>();
            for (Node n : cluster.nodes()) {
                if (n.id() != config.brokerId()) {
                    peerNodes.put(n.id(), n);
                    peerIds.add(n.id());
                }
            }

            this.raftTransport = new SocketRaftTransport(config.brokerId(), peerNodes,
                    config.raftConnectTimeoutMs(), config.raftReadTimeoutMs());
            this.controller = new Controller(config.brokerId(), cluster, config.raftLivenessTimeoutMs());
            RaftConfig raftConfig = new RaftConfig(20, config.raftHeartbeatMs(),
                    config.raftElectionMinMs(), config.raftElectionMaxMs());
            this.raftNode = new RaftNode(config.brokerId(), peerIds, raftTransport,
                    new MetadataStateMachine(cluster),
                    new FilePersistence(config.raftDir().resolve("broker-" + config.brokerId() + ".raft")),
                    raftConfig, controller);
            controller.setRaftNode(raftNode);
            handler.setRaftNode(raftNode);
        } else {
            this.raftTransport = null;
            this.controller = null;
            this.raftNode = null;
        }
    }

    public void start() {
        LOG.info("starting broker {} on {}:{} (cluster of {} broker(s), {} topic(s), raft={})",
                config.brokerId(), config.host(), config.port(),
                cluster.brokerCount(), cluster.topicNames().size(), config.raftEnabled());

        // Set up local partitions and start replica fetchers before we accept client traffic.
        replicaManager.start();
        groupCoordinator.start();
        server.start();
        if (raftNode != null) {
            raftNode.start();
            controller.start();
        }

        background = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "mk-background-b" + config.brokerId());
            t.setDaemon(true);
            return t;
        });
        background.scheduleAtFixedRate(this::flushQuietly, 1, 1, TimeUnit.SECONDS);
        background.scheduleAtFixedRate(() -> LOG.info("metrics: {}", metrics.snapshot()), 10, 10, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(this::close, "mk-shutdown-b" + config.brokerId()));
        LOG.info("broker {} started", config.brokerId());
    }

    private void flushQuietly() {
        try {
            logManager.flushAll();
        } catch (RuntimeException e) {
            LOG.warn("flush failed: {}", e.toString());
        }
    }

    public synchronized void close() {
        if (closed) {
            return; // idempotent: safe to call explicitly and again from the JVM shutdown hook
        }
        closed = true;
        LOG.info("shutting down broker {}", config.brokerId());
        server.shutdown();
        groupCoordinator.stop();
        if (controller != null) {
            controller.stop();
        }
        if (raftNode != null) {
            raftNode.stop();
        }
        if (raftTransport != null) {
            raftTransport.stop();
        }
        replicaManager.shutdown();
        if (background != null) {
            background.shutdownNow();
        }
        logManager.flushAll();
        logManager.closeAll();
        LOG.info("broker {} stopped", config.brokerId());
    }

    public Metrics metrics() {
        return metrics;
    }

    public int port() {
        return config.port();
    }
}
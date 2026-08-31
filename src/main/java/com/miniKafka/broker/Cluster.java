package com.minikafka.broker;

import com.minikafka.common.Node;
import com.minikafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The shared view of the cluster: which brokers exist and which topics are defined. Every broker
 * builds an identical {@link Cluster} from its (identical) config, then uses {@link PartitionAssignor}
 * to derive leader/replica placement. Because the derivation is deterministic, no coordination is
 * required for brokers to agree.
 *
 * <p>New topics created at runtime via CREATE_TOPICS are added here and propagated to peers so the
 * view stays consistent.
 */
public final class Cluster {
    private final List<Node> brokers;
    private final int[] sortedBrokerIds;
    private final Map<String, TopicConfig> topics = new ConcurrentHashMap<>();

    /** Failover overrides applied by the Raft controller: which replica currently leads a partition.
     * Empty by default, so leadership falls back to the deterministic assignment. */
    private final Map<TopicPartition, Integer> leaderOverride = new ConcurrentHashMap<>();

    public Cluster(List<Node> brokers) {
        List<Node> sorted = new ArrayList<>(brokers);
        sorted.sort(Comparator.comparingInt(Node::id));
        this.brokers = List.copyOf(sorted);
        this.sortedBrokerIds = sorted.stream().mapToInt(Node::id).toArray();
    }

    public List<Node> nodes() {
        return brokers;
    }

    public Node nodeById(int id) {
        for (Node n : brokers) {
            if (n.id() == id) {
                return n;
            }
        }
        return null;
    }

    public int brokerCount() {
        return brokers.size();
    }

    public void addOrUpdateTopic(TopicConfig topic) {
        topics.put(topic.name, topic);
    }

    public TopicConfig getTopic(String name) {
        return topics.get(name);
    }

    public boolean hasTopic(String name) {
        return topics.containsKey(name);
    }

    public List<String> topicNames() {
        return new ArrayList<>(topics.keySet());
    }

    /** The replica broker-ids for a partition (leader first), or null if the topic/partition is unknown. */
    public int[] replicas(TopicPartition tp) {
        TopicConfig tc = topics.get(tp.topic());
        if (tc == null || tp.partition() < 0 || tp.partition() >= tc.numPartitions) {
            return null;
        }
        int[][] assignment = PartitionAssignor.assign(sortedBrokerIds, tc.numPartitions, tc.replicationFactor);
        return assignment[tp.partition()];
    }

    public int leader(TopicPartition tp) {
        Integer override = leaderOverride.get(tp);
        if (override != null) {
            return override;
        }
        int[] replicas = replicas(tp);
        return replicas == null ? -1 : replicas[0];
    }

    /** Applied by the metadata state machine when the controller fails a partition over to a new leader. */
    public void setLeaderOverride(TopicPartition tp, int leaderId) {
        leaderOverride.put(tp, leaderId);
    }

    public Integer leaderOverride(TopicPartition tp) {
        return leaderOverride.get(tp);
    }

    public boolean isLeader(int brokerId, TopicPartition tp) {
        return leader(tp) == brokerId;
    }

    public boolean isReplica(int brokerId, TopicPartition tp) {
        int[] replicas = replicas(tp);
        if (replicas == null) {
            return false;
        }
        for (int id : replicas) {
            if (id == brokerId) {
                return true;
            }
        }
        return false;
    }

    /** All partitions this broker should host (as leader or follower), across every known topic. */
    public List<TopicPartition> partitionsForBroker(int brokerId) {
        List<TopicPartition> result = new ArrayList<>();
        for (TopicConfig tc : topics.values()) {
            for (int p = 0; p < tc.numPartitions; p++) {
                TopicPartition tp = new TopicPartition(tc.name, p);
                if (isReplica(brokerId, tp)) {
                    result.add(tp);
                }
            }
        }
        return result;
    }
}
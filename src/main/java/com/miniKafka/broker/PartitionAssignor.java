package com.minikafka.broker;

/**
 * Deterministically assigns partition replicas to brokers.
 *
 * <p>This is the trick that lets the cluster agree on who leads what <em>without</em> a central
 * controller or consensus service: every broker runs the exact same function over the exact same
 * sorted broker-id list and topic config, so they all independently arrive at the same layout.
 *
 * <p>For partition {@code p} the replica set is {@code brokerIds[(p+r) % n]} for {@code r} in
 * {@code [0, rf)}. The first replica ({@code r == 0}) is the leader, so leadership is spread evenly
 * across brokers as the partition index increases.
 */
public final class PartitionAssignor {

    private PartitionAssignor() {
    }

    /**
     * @param sortedBrokerIds broker ids in ascending order (must be identical on every broker)
     * @param numPartitions number of partitions in the topic
     * @param replicationFactor desired replicas per partition (clamped to the broker count)
     * @return result[p] is the replica broker-id list for partition {@code p}, leader first
     */
    public static int[][] assign(int[] sortedBrokerIds, int numPartitions, int replicationFactor) {
        int n = sortedBrokerIds.length;
        if (n == 0) {
            throw new IllegalArgumentException("no brokers to assign to");
        }
        int rf = Math.max(1, Math.min(replicationFactor, n));
        int[][] result = new int[numPartitions][rf];
        for (int p = 0; p < numPartitions; p++) {
            for (int r = 0; r < rf; r++) {
                result[p][r] = sortedBrokerIds[(p + r) % n];
            }
        }
        return result;
    }
}
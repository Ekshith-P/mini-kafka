package com.minikafka.broker;

/** Static definition of a topic: its name, partition count, and desired replication factor. */
public final class TopicConfig {
    public final String name;
    public final int numPartitions;
    public final int replicationFactor;

    public TopicConfig(String name, int numPartitions, int replicationFactor) {
        this.name = name;
        this.numPartitions = numPartitions;
        this.replicationFactor = replicationFactor;
    }

    @Override
    public String toString() {
        return name + "(partitions=" + numPartitions + ", rf=" + replicationFactor + ")";
    }
}
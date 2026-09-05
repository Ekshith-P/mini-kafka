package com.minikafka.protocol.messages;

import com.minikafka.common.Errors;
import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.Protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/** METADATA response: the cluster's brokers and, per topic, the leader/replicas/ISR of each partition. */
public final class MetadataResponse {

    public static final class Broker {
        public final int nodeId;
        public final String host;
        public final int port;

        public Broker(int nodeId, String host, int port) {
            this.nodeId = nodeId;
            this.host = host;
            this.port = port;
        }
    }

    public static final class PartitionMetadata {
        public final short errorCode;
        public final int index;
        public final int leaderId;
        public final int[] replicas;
        public final int[] isr;

        public PartitionMetadata(short errorCode, int index, int leaderId, int[] replicas, int[] isr) {
            this.errorCode = errorCode;
            this.index = index;
            this.leaderId = leaderId;
            this.replicas = replicas;
            this.isr = isr;
        }
    }

    public static final class TopicMetadata {
        public final short errorCode;
        public final String name;
        public final List<PartitionMetadata> partitions;

        public TopicMetadata(short errorCode, String name, List<PartitionMetadata> partitions) {
            this.errorCode = errorCode;
            this.name = name;
            this.partitions = partitions;
        }
    }

    public final List<Broker> brokers;
    public final List<TopicMetadata> topics;

    public MetadataResponse(List<Broker> brokers, List<TopicMetadata> topics) {
        this.brokers = brokers;
        this.topics = topics;
    }

    public void writeTo(ByteWriter out) {
        out.putInt(brokers.size());
        for (Broker broker : brokers) {
            out.putInt(broker.nodeId);
            out.putString(broker.host);
            out.putInt(broker.port);
        }
        out.putInt(topics.size());
        for (TopicMetadata topic : topics) {
            out.putShort(topic.errorCode);
            out.putString(topic.name);
            out.putInt(topic.partitions.size());
            for (PartitionMetadata partition : topic.partitions) {
                out.putShort(partition.errorCode);
                out.putInt(partition.index);
                out.putInt(partition.leaderId);
                out.putIntArray(partition.replicas);
                out.putIntArray(partition.isr);
            }
        }
    }

    public static MetadataResponse parse(ByteBuffer buf) {
        int brokerCount = buf.getInt();
        List<Broker> brokers = new ArrayList<>(Math.max(0, brokerCount));
        for (int i = 0; i < brokerCount; i++) {
            int nodeId = buf.getInt();
            String host = Protocol.getString(buf);
            int port = buf.getInt();
            brokers.add(new Broker(nodeId, host, port));
        }
        int topicCount = buf.getInt();
        List<TopicMetadata> topics = new ArrayList<>(Math.max(0, topicCount));
        for (int t = 0; t < topicCount; t++) {
            short errorCode = buf.getShort();
            String name = Protocol.getString(buf);
            int partitionCount = buf.getInt();
            List<PartitionMetadata> partitions = new ArrayList<>(Math.max(0, partitionCount));
            for (int p = 0; p < partitionCount; p++) {
                short pErr = buf.getShort();
                int index = buf.getInt();
                int leaderId = buf.getInt();
                int[] replicas = Protocol.getIntArray(buf);
                int[] isr = Protocol.getIntArray(buf);
                partitions.add(new PartitionMetadata(pErr, index, leaderId, replicas, isr));
            }
            topics.add(new TopicMetadata(errorCode, name, partitions));
        }
        return new MetadataResponse(brokers, topics);
    }

    /** Finds the leader broker id for a partition, or -1 if unknown. */
    public int leaderFor(String topic, int partition) {
        for (TopicMetadata t : topics) {
            if (t.name.equals(topic) && t.errorCode == Errors.NONE) {
                for (PartitionMetadata p : t.partitions) {
                    if (p.index == partition) {
                        return p.leaderId;
                    }
                }
            }
        }
        return -1;
    }

    public Broker broker(int nodeId) {
        for (Broker b : brokers) {
            if (b.nodeId == nodeId) {
                return b;
            }
        }
        return null;
    }
}
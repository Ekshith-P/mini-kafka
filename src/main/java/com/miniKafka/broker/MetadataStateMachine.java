package com.minikafka.broker;

import com.minikafka.common.Log;
import com.minikafka.common.TopicPartition;
import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.Protocol;
import com.minikafka.raft.StateMachine;

import java.nio.ByteBuffer;

/**
 * The Raft state machine for cluster control metadata. Right now it carries one command type -
 * {@code LeaderAndIsr}, emitted by the controller when it fails a partition over to a new leader.
 *
 * <p>Because Raft delivers the same committed commands in the same order to every broker, each
 * broker's {@link Cluster} ends up with an identical leader-override map - a consistent, cluster-wide
 * view of who leads what, with no separate coordination.
 */
public final class MetadataStateMachine implements StateMachine {
    private static final Log LOG = Log.of(MetadataStateMachine.class);
    private static final byte LEADER_AND_ISR = 1;

    private final Cluster cluster;

    public MetadataStateMachine(Cluster cluster) {
        this.cluster = cluster;
    }

    /** Encodes a "partition {@code tp} is now led by {@code leader} with in-sync set {@code isr}". */
    public static byte[] leaderAndIsr(TopicPartition tp, int leader, int[] isr) {
        ByteWriter w = new ByteWriter();
        w.putByte(LEADER_AND_ISR);
        w.putString(tp.topic());
        w.putInt(tp.partition());
        w.putInt(leader);
        w.putIntArray(isr);
        return w.toByteArray();
    }

    @Override
    public void apply(long index, byte[] command) {
        ByteBuffer b = ByteBuffer.wrap(command);
        byte type = b.get();
        if (type == LEADER_AND_ISR) {
            String topic = Protocol.getString(b);
            int partition = b.getInt();
            int leader = b.getInt();
            Protocol.getIntArray(b); // isr - currently informational; the new leader re-establishes it
            TopicPartition tp = new TopicPartition(topic, partition);
            cluster.setLeaderOverride(tp, leader);
            LOG.info("metadata: {} leader is now broker {}", tp, leader);
        }
    }
}
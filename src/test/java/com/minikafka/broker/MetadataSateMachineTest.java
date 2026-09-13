package com.minikafka.broker;

import com.minikafka.common.Node;
import com.minikafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class MetadataStateMachineTest {

    @Test
    void applyingLeaderAndIsrOverridesTheDeterministicLeader() {
        Cluster cluster = new Cluster(List.of(
                new Node(1, "h", 1), new Node(2, "h", 2), new Node(3, "h", 3)));
        cluster.addOrUpdateTopic(new TopicConfig("t", 3, 3));
        TopicPartition tp = new TopicPartition("t", 0);

        int deterministicLeader = cluster.leader(tp); // partition 0 -> broker 1 by assignment
        assertEquals(1, deterministicLeader);

        MetadataStateMachine sm = new MetadataStateMachine(cluster);
        sm.apply(1, MetadataStateMachine.leaderAndIsr(tp, 2, new int[]{2, 3}));

        assertEquals(2, cluster.leader(tp), "override should take effect");
        assertNotEquals(deterministicLeader, cluster.leader(tp));
        // other partitions are untouched
        assertEquals(2, cluster.leader(new TopicPartition("t", 1)));
    }
}
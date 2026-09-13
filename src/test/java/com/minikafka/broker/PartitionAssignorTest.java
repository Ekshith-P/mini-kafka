package com.minikafka.broker;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PartitionAssignorTest {

    @Test
    void spreadsLeadersAndWrapsReplicas() {
        int[][] assignment = PartitionAssignor.assign(new int[]{1, 2, 3}, 3, 3);
        // Leaders (first replica) rotate across brokers as partition index increases.
        assertEquals(1, assignment[0][0]);
        assertEquals(2, assignment[1][0]);
        assertEquals(3, assignment[2][0]);
        // Replica sets wrap around the broker list.
        assertArrayEquals(new int[]{1, 2, 3}, assignment[0]);
        assertArrayEquals(new int[]{2, 3, 1}, assignment[1]);
        assertArrayEquals(new int[]{3, 1, 2}, assignment[2]);
    }

    @Test
    void replicationFactorClampedToBrokerCount() {
        int[][] assignment = PartitionAssignor.assign(new int[]{1, 2}, 4, 5);
        for (int[] replicas : assignment) {
            assertEquals(2, replicas.length);
        }
    }

    @Test
    void isDeterministic() {
        int[][] a = PartitionAssignor.assign(new int[]{10, 20, 30}, 6, 2);
        int[][] b = PartitionAssignor.assign(new int[]{10, 20, 30}, 6, 2);
        for (int p = 0; p < a.length; p++) {
            assertArrayEquals(a[p], b[p]);
        }
    }
}
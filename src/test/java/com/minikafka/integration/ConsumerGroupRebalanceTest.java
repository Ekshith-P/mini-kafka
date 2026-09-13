
package com.minikafka.integration;

import com.minikafka.broker.Broker;
import com.minikafka.broker.BrokerConfig;
import com.minikafka.client.Consumer;
import com.minikafka.client.Producer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end consumer-group test: two consumers split a topic's partitions, share the load, and one
 * takes over everything when the other leaves. Slow integration test (tens of seconds).
 */
class ConsumerGroupRebalanceTest {

    /** A group member with its own poll loop on a background thread. */
    private static final class Runner {
        final Consumer consumer;
        final Set<String> collected = ConcurrentHashMap.newKeySet();
        volatile boolean running = true;
        Thread thread;

        Runner(String bootstrap, String group) {
            this.consumer = new Consumer(bootstrap, group, true);
        }

        void start(String topic) {
            thread = new Thread(() -> {
                try {
                    consumer.subscribe(topic);
                    while (running) {
                        for (Consumer.ConsumerRecord r : consumer.poll()) {
                            collected.add(r.valueAsString());
                        }
                        Thread.sleep(50);
                    }
                } catch (Exception ignored) {
                    // stopping
                }
            });
            thread.setDaemon(true);
            thread.start();
        }

        void stop() {
            running = false;
            try {
                thread.join(3000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            consumer.close();
        }

        Set<Integer> assignment() {
            return new HashSet<>(consumer.assignedPartitions());
        }

        long count(String prefix) {
            return collected.stream().filter(s -> s.startsWith(prefix)).count();
        }
    }

    @Test
    void twoConsumersShareThenOneTakesOver(@TempDir Path data) throws Exception {
        int port;
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }
        Properties p = new Properties();
        p.setProperty("broker.id", "1");
        p.setProperty("listeners.host", "localhost");
        p.setProperty("listeners.port", String.valueOf(port));
        p.setProperty("log.dirs", data.toString());
        p.setProperty("cluster.brokers", "1:localhost:" + port);
        p.setProperty("topics", "grp:3:1");
        Broker broker = new Broker(BrokerConfig.fromProperties(p));
        broker.start();
        String bootstrap = "localhost:" + port;

        Runner a = new Runner(bootstrap, "G");
        Runner b = new Runner(bootstrap, "G");
        try {
            a.start("grp");
            Thread.sleep(1000);
            b.start("grp");
            Thread.sleep(3000); // rebalance settles

            Set<Integer> aP = a.assignment();
            Set<Integer> bP = b.assignment();
            Set<Integer> union = new HashSet<>(aP);
            union.addAll(bP);
            assertTrue(Collections.disjoint(aP, bP), "assignments overlap: " + aP + " / " + bP);
            assertEquals(Set.of(0, 1, 2), union, "partitions must be fully covered");
            assertTrue(!aP.isEmpty() && !bP.isEmpty(), "both consumers should own partitions");

            try (Producer prod = new Producer(bootstrap, (short) 1)) {
                for (int i = 0; i < 30; i++) {
                    prod.send("grp", "w" + i, "w" + i);
                }
            }
            Thread.sleep(3000);
            Set<String> w = new HashSet<>();
            a.collected.forEach(s -> {
                if (s.startsWith("w")) {
                    w.add(s);
                }
            });
            b.collected.forEach(s -> {
                if (s.startsWith("w")) {
                    w.add(s);
                }
            });
            assertEquals(30, w.size(), "all 30 post-split records should be consumed");
            assertTrue(a.count("w") > 0 && b.count("w") > 0, "load should be split across both consumers");

            b.stop();
            Thread.sleep(3000);
            assertEquals(Set.of(0, 1, 2), a.assignment(), "A should take over all partitions after B leaves");

            try (Producer prod = new Producer(bootstrap, (short) 1)) {
                for (int i = 0; i < 10; i++) {
                    prod.send("grp", "x" + i, "x" + i);
                }
            }
            Thread.sleep(3000);
            assertEquals(10, a.count("x"), "sole consumer should read all new records");
        } finally {
            a.stop();
            try {
                b.stop();
            } catch (Exception ignored) {
                // already stopped
            }
            broker.close();
        }
    }
}
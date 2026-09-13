package com.minikafka.integration;

import com.minikafka.broker.Broker;
import com.minikafka.broker.BrokerConfig;
import com.minikafka.client.Consumer;
import com.minikafka.client.Producer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end test on a real single broker: start it on an ephemeral port, produce records through
 * the TCP protocol, and consume them all back.
 */
class ProduceConsumeTest {

    @Test
    void produceThenConsumeRoundTrip(@TempDir Path dataDir) throws Exception {
        int port = freePort();
        Broker broker = new Broker(config(port, dataDir));
        broker.start();
        try {
            String bootstrap = "localhost:" + port;

            try (Producer producer = new Producer(bootstrap, (short) 1)) {
                for (int i = 0; i < 30; i++) {
                    Producer.RecordMetadata md = producer.send("it", "key-" + i, "value-" + i);
                    assertTrue(md.offset >= 0);
                }
            }

            Set<String> received = new HashSet<>();
            try (Consumer consumer = new Consumer(bootstrap, "it-group", true)) {
                consumer.subscribe("it");
                assertEquals(2, consumer.assignedPartitions().size());
                long deadline = System.currentTimeMillis() + 5000;
                while (received.size() < 30 && System.currentTimeMillis() < deadline) {
                    for (Consumer.ConsumerRecord r : consumer.poll()) {
                        received.add(r.valueAsString());
                    }
                    if (received.size() < 30) {
                        Thread.sleep(50);
                    }
                }
                consumer.commitSync();
            }

            assertEquals(30, received.size());
            for (int i = 0; i < 30; i++) {
                assertTrue(received.contains("value-" + i), "missing value-" + i);
            }
        } finally {
            broker.close();
        }
    }

    @Test
    void committedOffsetsSurviveNewConsumer(@TempDir Path dataDir) throws Exception {
        int port = freePort();
        Broker broker = new Broker(config(port, dataDir));
        broker.start();
        try {
            String bootstrap = "localhost:" + port;
            try (Producer producer = new Producer(bootstrap, (short) 1)) {
                for (int i = 0; i < 10; i++) {
                    producer.send("it", null, "m" + i);
                }
            }

            // First consumer reads everything and commits.
            try (Consumer consumer = new Consumer(bootstrap, "grp", true)) {
                consumer.subscribe("it");
                int seen = 0;
                long deadline = System.currentTimeMillis() + 5000;
                while (seen < 10 && System.currentTimeMillis() < deadline) {
                    seen += consumer.poll().size();
                    Thread.sleep(30);
                }
                consumer.commitSync();
                assertEquals(10, seen);
            }

            // A new consumer in the same group resumes after the committed offsets: nothing new.
            try (Consumer consumer = new Consumer(bootstrap, "grp", true)) {
                consumer.subscribe("it");
                int seen = 0;
                for (int i = 0; i < 5; i++) {
                    seen += consumer.poll().size();
                    Thread.sleep(30);
                }
                assertEquals(0, seen);
            }
        } finally {
            broker.close();
        }
    }

    private static BrokerConfig config(int port, Path dataDir) {
        Properties p = new Properties();
        p.setProperty("broker.id", "1");
        p.setProperty("listeners.host", "localhost");
        p.setProperty("listeners.port", String.valueOf(port));
        p.setProperty("log.dirs", dataDir.toString());
        p.setProperty("cluster.brokers", "1:localhost:" + port);
        p.setProperty("topics", "it:2:1");
        return BrokerConfig.fromProperties(p);
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
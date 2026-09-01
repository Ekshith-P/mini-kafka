package com.minikafka.client.cli;

import com.minikafka.client.Consumer;
import com.minikafka.client.KafkaClient;
import com.minikafka.client.Producer;
import com.minikafka.common.ApiKeys;
import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.messages.CreateTopicsRequest;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A simple throughput benchmark:
 * {@code bench --bootstrap host:port [--topic T] [--records N] [--size B] [--acks 1]
 *          [--partitions P] [--replication R] [--create] [--skip-consume]}.
 *
 * <p>It measures <i>synchronous</i> produce throughput (each send waits for the broker's ack over a
 * single connection) and then consume throughput reading the records back. These are the honest
 * end-to-end numbers to quote – real Kafka clients batch and pipeline for higher figures, which is
 * noted as a natural optimization.
 */
public final class BenchmarkCommand {

    private BenchmarkCommand() {
    }

    public static void run(String[] argv) throws Exception {
        CliArgs args = CliArgs.parse(argv);
        String bootstrap = args.get("bootstrap", "localhost:9092");
        String topic = args.get("topic", "bench");
        int records = args.getInt("records", 200_000);
        int size = args.getInt("size", 100);
        short acks = (short) args.getInt("acks", 1);
        int partitions = args.getInt("partitions", 3);
        int replication = args.getInt("replication", 1);
        boolean create = args.has("create");
        boolean skipConsume = args.has("skip-consume");

        if (create) {
            createTopic(bootstrap, topic, partitions, replication);
            Thread.sleep(500); // let assignment/replication settle
        }

        byte[] value = new byte[size];
        Arrays.fill(value, (byte) 'x');

        System.out.printf("producing %,d records of %d bytes (acks=%d) to '%s'...\n",
                records, size, acks, topic);
        long producedBytes = 0;
        long start = System.nanoTime();
        try (Producer producer = new Producer(bootstrap, acks)) {
            for (int i = 0; i < records; i++) {
                producer.send(topic, (byte[]) null, value);
                producedBytes += size;
            }
        }
        double produceSec = (System.nanoTime() - start) / 1e9;
        report("PRODUCE", records, producedBytes, produceSec);

        if (skipConsume) {
            return;
        }

        System.out.printf("\nconsuming records back from '%s'...\n", topic);
        long consumeStart = System.nanoTime();
        long consumed = 0;
        long consumedBytes = 0;
        try (Consumer consumer = new Consumer(bootstrap, "bench-" + System.currentTimeMillis(), true)) {
            consumer.subscribe(topic);
            int idlePolls = 0;
            while (consumed < records && idlePolls < 50) { // ~10s of idle tolerance at 200ms
                List<Consumer.ConsumerRecord> batch = consumer.poll();
                if (batch.isEmpty()) {
                    idlePolls++;
                    Thread.sleep(200);
                    continue;
                }
                idlePolls = 0;
                consumed += batch.size();
                for (Consumer.ConsumerRecord r : batch) {
                    consumedBytes += r.value == null ? 0 : r.value.length;
                }
            }
        }
        double consumeSec = (System.nanoTime() - consumeStart) / 1e9;
        report("CONSUME", consumed, consumedBytes, consumeSec);
        if (consumed < records) {
            System.out.printf("  (note: only %,d of %,d records were read back)\n", consumed, records);
        }
    }

    private static void createTopic(String bootstrap, String topic, int partitions, int replication)
            throws Exception {
        CreateTopicsRequest request = new CreateTopicsRequest(
                Collections.singletonList(new CreateTopicsRequest.Topic(topic, partitions, (short) replication)),
                30000);
        ByteWriter body = new ByteWriter();
        request.writeTo(body);
        try (KafkaClient client = new KafkaClient(bootstrap, "mini-kafka-bench-admin", 5000)) {
            client.sendToAny(ApiKeys.CREATE_TOPICS.id, body.toByteArray());
        }
        System.out.printf("ensured topic '%s' (partitions=%d, replication=%d)\n",
                topic, partitions, replication);
    }

    private static void report(String label, long messages, long bytes, double seconds) {
        double msgsPerSec = messages / seconds;
        double mbPerSec = (bytes / 1_000_000.0) / seconds;
        System.out.printf("%-8s %,d msgs in %.2fs => %,.0f msgs/sec, %.1f MB/sec\n",
                label, messages, seconds, msgsPerSec, mbPerSec);
    }
}
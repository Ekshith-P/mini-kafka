package com.minikafka.client.cli;

import com.minikafka.client.Consumer;

import java.util.List;

/**
 * {@code consume --bootstrap host:port --topic NAME [--group G] [--from-beginning]}.
 * Polls the topic and prints each record until interrupted (Ctrl-C).
 */
public final class ConsoleConsumer {

    private ConsoleConsumer() {
    }

    public static void run(String[] argv) throws Exception {
        CliArgs args = CliArgs.parse(argv);
        String bootstrap = args.get("bootstrap", "localhost:9092");
        String topic = args.require("topic");
        String group = args.get("group", "console-consumer");
        boolean fromBeginning = args.has("from-beginning");

        try (Consumer consumer = new Consumer(bootstrap, group, fromBeginning)) {
            consumer.subscribe(topic);
            System.out.printf("Consuming '%s' (group=%s, partitions=%s)%n",
                    topic, group, consumer.assignedPartitions());
            while (!Thread.currentThread().isInterrupted()) {
                List<Consumer.ConsumerRecord> records = consumer.poll();
                if (records.isEmpty()) {
                    Thread.sleep(200);
                    continue;
                }
                for (Consumer.ConsumerRecord record : records) {
                    String key = record.keyAsString();
                    System.out.printf("[%p%d @ %d]%s %s%n",
                            record.partition, record.offset,
                            key == null ? "" : " key=" + key,
                            record.valueAsString());
                }
                consumer.commitSync();
            }
        }
    }
}
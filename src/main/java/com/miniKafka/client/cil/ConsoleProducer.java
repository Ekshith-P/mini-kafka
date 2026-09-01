package com.minikafka.client.cli;

import com.minikafka.client.Producer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/**
 * {@code produce --bootstrap host:port --topic NAME [--acks 1] [--key-separator :]}.
 * Reads lines from stdin and sends each as a record. If a key separator is given and a line contains
 * it, the part before it is used as the key (so records with the same key stay on one partition).
 */
public final class ConsoleProducer {

    private ConsoleProducer() {
    }

    public static void run(String[] argv) throws Exception {
        CliArgs args = CliArgs.parse(argv);

        String bootstrap = args.get("bootstrap", "localhost:9092");
        String topic = args.get("topic", "test");
        short acks = (short) args.getInt("acks", 1);
        String separator = args.get("key-separator", null);

        System.out.println("Type messages, one per line (Ctrl-D to stop):");

        try (Producer producer = new Producer(bootstrap, acks);
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(System.in, StandardCharsets.UTF_8))) {

            String line;

            while ((line = reader.readLine()) != null) {
                String key = null;
                String value = line;

                if (separator != null) {
                    int idx = line.indexOf(separator);

                    if (idx >= 0) {
                        key = line.substring(0, idx);
                        value = line.substring(idx + separator.length());
                    }
                }

                Producer.RecordMetadata md =
                        producer.send(topic, key, value);

                System.out.printf(
                        " -> partition %d, offset %d%n",
                        md.partition,
                        md.offset
                );
            }
        }

        System.out.println("done.");
    }
}
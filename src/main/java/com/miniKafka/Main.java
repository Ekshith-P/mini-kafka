package com.minikafka;

import com.minikafka.broker.Broker;
import com.minikafka.broker.BrokerConfig;
import com.minikafka.client.cli.BenchmarkCommand;
import com.minikafka.client.cli.ConsoleConsumer;
import com.minikafka.client.cli.ConsoleProducer;
import com.minikafka.client.cli.CreateTopicCommand;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

/**
 * Single entrypoint for the whole project. The first argument selects a subcommand:
 * <pre>
 *   broker       &lt;config.properties&gt;   start a broker
 *   create-topic --bootstrap ... --topic ... [--partitions N] [--replication R]
 *   produce      --bootstrap ... --topic ... [--acks 1] [--key-separator :]
 *   consume      --bootstrap ... --topic ... [--group G] [--from-beginning]
 *   bench        --bootstrap ... [--topic T] [--records N] [--size B] [--create]
 * </pre>
 */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            printUsageAndExit();
            return;
        }

        String command = args[0];
        String[] rest = Arrays.copyOfRange(args, 1, args.length);

        switch (command) {
            case "broker":
                runBroker(rest);
                break;

            case "create-topic":
                CreateTopicCommand.run(rest);
                break;

            case "produce":
                ConsoleProducer.run(rest);
                break;

            case "consume":
                ConsoleConsumer.run(rest);
                break;

            case "bench":
                BenchmarkCommand.run(rest);
                break;

            default:
                System.err.println("unknown command: " + command);
                printUsageAndExit();
        }
    }

    private static void runBroker(String[] rest) throws InterruptedException {
        if (rest.length < 1) {
            System.err.println("usage: broker <config.properties>");
            System.exit(2);
            return;
        }

        BrokerConfig config = BrokerConfig.fromFile(Path.of(rest[0]));
        Broker broker = new Broker(config);
        broker.start();

        // Block forever; the JVM shutdown hook installed by Broker handles graceful cleanup.
        new CountDownLatch(1).await();
    }

    private static void printUsageAndExit() {
        System.err.println("mini-kafka — a Kafka-like broker built from scratch");
        System.err.println();
        System.err.println("commands:");
        System.err.println("  broker       <config.properties>");
        System.err.println("  create-topic --bootstrap host:port --topic NAME [--partitions N] [--replication R]");
        System.err.println("  produce      --bootstrap host:port --topic NAME [--acks 1] [--key-separator :]");
        System.err.println("  consume      --bootstrap host:port --topic NAME [--group G] [--from-beginning]");
        System.err.println("  bench        --bootstrap host:port [--topic T] [--records N] [--size B] [--create]");
        System.exit(2);
    }
}
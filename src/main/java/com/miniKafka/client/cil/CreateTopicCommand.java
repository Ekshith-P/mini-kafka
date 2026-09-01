package com.minikafka.client.cli;

import com.minikafka.client.KafkaClient;
import com.minikafka.common.ApiKeys;
import com.minikafka.common.Errors;
import com.minikafka.protocol.ByteWriter;
import com.minikafka.protocol.messages.CreateTopicsRequest;
import com.minikafka.protocol.messages.CreateTopicsResponse;

import java.nio.ByteBuffer;
import java.util.Collections;

/** {@code create-topic --bootstrap host:port --topic NAME [--partitions N] [--replication R]}. */
public final class CreateTopicCommand {

    private CreateTopicCommand() {
    }

    public static void run(String[] argv) throws Exception {
        CliArgs args = CliArgs.parse(argv);
        String bootstrap = args.get("bootstrap", "localhost:9092");
        String topic = args.require("topic");
        int partitions = args.getInt("partitions", 1);
        int replication = args.getInt("replication", 1);

        CreateTopicsRequest.Topic spec = new CreateTopicsRequest.Topic(topic, partitions, (short) replication);
        CreateTopicsRequest request = new CreateTopicsRequest(Collections.singletonList(spec), 30000);

        ByteWriter body = new ByteWriter();
        request.writeTo(body);

        try (KafkaClient client = new KafkaClient(bootstrap, "mini-kafka-admin", 5000)) {
            ByteBuffer buf = client.sendToAny(ApiKeys.CREATE_TOPICS.id, body.toByteArray());
            CreateTopicsResponse resp = CreateTopicsResponse.parse(buf);
            CreateTopicsResponse.Topic result = resp.topics.get(0);
            if (result.errorCode == Errors.NONE) {
                System.out.printf("created topic '%s' (partitions=%d, replication=%d)%n",
                        topic, partitions, replication);
            } else {
                System.out.printf("failed to create topic '%s': %s%n",
                        topic, Errors.message(result.errorCode));
            }
        }
    }
}